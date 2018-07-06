package akka.persistence.kafka.journal

import java.util.UUID

import akka.persistence.journal.{AsyncWriteJournal, Tagged}
import akka.persistence.{AtomicWrite, PersistentRepr}
import com.evolutiongaming.cassandra.{CassandraConfig, CreateCluster}
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.kafka.journal.eventual.{Eventual, EventualDb}
import com.evolutiongaming.kafka.journal.eventual.cassandra.{EventualCassandra, EventualCassandraConfig, SchemaConfig}
import com.evolutiongaming.kafka.journal.{Client, Entry, SeqRange}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.serialization.{SerializedMsg, SerializedMsgExt}
import com.evolutiongaming.skafka.Bytes
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig, CreateConsumer}
import com.evolutiongaming.skafka.producer.{CreateProducer, ProducerConfig}

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try
import scala.util.control.NonFatal

class KafkaJournal extends AsyncWriteJournal {
  import KafkaJournal._

  val serializedMsgExt = SerializedMsgExt(context.system)
  implicit val system = context.system
  implicit val ec = system.dispatcher

  val log = ActorLog(system, classOf[KafkaJournal])

  lazy val client: Client = {

    def config(name: String) = {
      val config = system.settings.config
      val common = config.getConfig("kafka.persistence.journal.kafka")
      common.getConfig(name) withFallback common
    }

    val producerConfig = ProducerConfig(config("producer"))
    log.debug(s"Producer config: $producerConfig")

    val ecBlocking = system.dispatchers.lookup("kafka-plugin-blocking-dispatcher")

    val producer = CreateProducer(producerConfig, ecBlocking)

    system.registerOnTermination {
      val future = for {
        _ <- producer.flush()
        _ <- producer.closeAsync(3.seconds)
      } yield ()
      try Await.result(future, 5.seconds) catch {
        case NonFatal(failure) => log.error(s"failed to shutdown producer $failure", failure)
      }
    }
    
    val consumerConfig = ConsumerConfig(config("consumer"))
    log.debug(s"Consumer config: $consumerConfig")

    val newConsumer = () => {
      val groupId = UUID.randomUUID().toString
      val configFixed = consumerConfig.copy(
        groupId = Some(groupId),
        autoOffsetReset = AutoOffsetReset.Earliest)
      CreateConsumer[String, Bytes](configFixed, ecBlocking)
    }

    val eventual: Eventual = {
      val cassandraConfig = CassandraConfig.Default
      val cluster = CreateCluster(cassandraConfig)
      val session = cluster.connect()
      val schemaConfig = SchemaConfig.Default
      val config = EventualCassandraConfig.Default
      // TODO read only cassandra statements
      EventualCassandra(session, schemaConfig, config)
    }

    Client(producer, newConsumer, eventual)
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
    
    val range = SeqRange(from , to)

    log.debug(s"asyncReplayMessages persistenceId: $persistenceId, range: $range, max: $max")

    client.read(persistenceId, range).map { entries =>
      val maxInt = (max min Int.MaxValue).toInt
      val filtered = entries.take(maxInt) // TODO avoid reading more than needed

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
    client.lastSeqNr(persistenceId, from).map { seqNr =>
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
