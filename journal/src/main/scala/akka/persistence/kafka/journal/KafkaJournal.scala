package akka.persistence.kafka.journal

import java.util.{Properties, UUID}

import akka.persistence.journal.{AsyncWriteJournal, Tagged}
import akka.persistence.{AtomicWrite, PersistentRepr}
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.kafka.journal.Aliases._
import com.evolutiongaming.kafka.journal.{Client, Entry}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.serialization.{SerializedMsg, SerializedMsgExt}
import com.evolutiongaming.skafka.producer.{Configs, CreateProducer}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.util.Try

class KafkaJournal extends AsyncWriteJournal {
  import KafkaJournal._

  val serializedMsgExt = SerializedMsgExt(context.system)
  implicit val system = context.system
  implicit val ec = system.dispatcher

  val log = ActorLog(system, classOf[KafkaJournal])

  lazy val client: Client = {
    val ecBlocking = system.dispatchers.lookup("kafka-plugin-blocking-dispatcher")
    val configs = Configs.Default
    val producer = CreateProducer(configs, ecBlocking)
    val newConsumer = () => {
      val groupId = UUID.randomUUID().toString
      val props = new Properties()
      props.put("bootstrap.servers", "localhost:9092")
      props.put("group.id", groupId)
      props.put("enable.auto.commit", "false")
      props.put("auto.offset.reset", "earliest")
      //  props.put("zookeeper.connect", "localhost:2181");
      //  props.put("auto.commit.interval.ms", "1000")
      //  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      //  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

      val deserializerStr = new StringDeserializer()
      val deserializerBin = new ByteArrayDeserializer()
      val consumer = new KafkaConsumer(props, deserializerStr, deserializerBin)
      com.evolutiongaming.skafka.concumer.Consumer(consumer)
    }
    Client(producer, newConsumer)
  }

  // TODO optimise sequence of calls asyncWriteMessages & asyncReadHighestSequenceNr for the same persistenceId

  def asyncWriteMessages(atomicWrites: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    val persistentReprs = for {
      atomicWrite <- atomicWrites
      persistentRepr <- atomicWrite.payload
    } yield {
      persistentRepr
    }
    if (persistentReprs.isEmpty) FutureNil
    else {
      val persistenceId = persistentReprs.head.persistenceId

      def seqNrs = persistentReprs.map(_.sequenceNr).mkString(",")

      log.debug(s"asyncWriteMessages persistenceId: $persistenceId, seqNrs: $seqNrs")

      val result = Future {
        val records = for {
          persistentRepr <- persistentReprs
        } yield {
          val (payload: AnyRef, tags) = PayloadAndTags(persistentRepr.payload)
          val serialized = serializedMsgExt.toMsg(payload)
          val persistentEvent = PersistentEvent(serialized, persistentRepr)
          val bytes = PersistentEventSerializer.toBinary(persistentEvent)
          // TODO rename
          Entry(bytes, persistentRepr.sequenceNr, tags)
        }
        val result = client.append(persistenceId, Nel(records.head, records.tail.toList))
        result.map(_ => Nil)(CurrentThreadExecutionContext)
      }
      result.flatMap(identity)(CurrentThreadExecutionContext)
    }
  }

  def asyncDeleteMessagesTo(persistenceId: PersistenceId, to: SeqNr): Future[Unit] = {

    log.debug(s"asyncDeleteMessagesTo persistenceId: $persistenceId, to: $to")

    client.truncate(persistenceId, to)
  }

  def asyncReplayMessages(persistenceId: PersistenceId, from: SeqNr, to: SeqNr, max: Long)
    (callback: PersistentRepr => Unit): Future[Unit] = {

    log.debug(s"asyncReplayMessages persistenceId: $persistenceId, from: $from, to: $to, max: $max")

    client.read(persistenceId).map { entries =>
      val maxInt = (max min Int.MaxValue).toInt
      val filtered = entries.filter(x => x.seqNr >= from && x.seqNr <= to).take(maxInt)

      def seqNrs = filtered.map(_.seqNr).mkString(",")

      log.debug(s"asyncReplayMessages persistenceId: $persistenceId, from: $from, to: $to, max: $max, result: $seqNrs")

      val persistentReprs = for {
        entry <- filtered
      } yield {
        val persistentEvent = PersistentEventSerializer.fromBinary(entry.payload)
        val serializedMsg = SerializedMsg(persistentEvent.identifier, persistentEvent.manifest, persistentEvent.payload)
        val payload = serializedMsgExt.fromMsg(serializedMsg).get
        PersistentRepr(
          payload = payload,
          sequenceNr = persistentEvent.seqNr,
          persistenceId = persistenceId,
          manifest = persistentEvent.persistentManifest,
          writerUuid = persistentEvent.writerUuid)
      }
      for {persistentRepr <- persistentReprs} callback(persistentRepr)
    }
  }

  def asyncReadHighestSequenceNr(persistenceId: PersistenceId, from: SeqNr): Future[SeqNr] = {
    client.lastSeqNr(persistenceId).map { seqNr =>
      log.debug(s"asyncReadHighestSequenceNr persistenceId: $persistenceId, from: $from, result: $seqNr")
      seqNr
    }
  }
}

object KafkaJournal {
  val FutureNil: Future[List[Nothing]] = Future.successful(Nil)
}

case class PersistentEvent(
  seqNr: SeqNr, // TODO
  persistentManifest: String,
  writerUuid: String,
  identifier: Int,
  manifest: String,
  payload: Array[Byte])


object PersistentEvent {

  def apply(msg: SerializedMsg, persistentRepr: PersistentRepr): PersistentEvent = {
    PersistentEvent(
      seqNr = persistentRepr.sequenceNr,
      persistentManifest = persistentRepr.manifest,
      writerUuid = persistentRepr.writerUuid,
      identifier = msg.identifier,
      manifest = msg.manifest,
      payload = msg.bytes)
  }
}


object PayloadAndTags {
  def apply(payload: Any): (Any, Set[String]) = payload match {
    case Tagged(payload, tags) => (payload, tags)
    case _                     => (payload, Set.empty)
  }
}
