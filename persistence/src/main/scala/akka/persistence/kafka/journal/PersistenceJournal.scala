package akka.persistence.kafka.journal

import java.time.Instant
import java.util.UUID

import akka.persistence.journal.{AsyncWriteJournal, Tagged}
import akka.persistence.{AtomicWrite, PersistentRepr}
import com.evolutiongaming.cassandra.{CassandraConfig, CreateCluster}
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.kafka.journal.FutureHelper._
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.eventual.cassandra.{EventualCassandra, EventualCassandraConfig, SchemaConfig}
import com.evolutiongaming.kafka.journal.{Bytes, Event, Journals}
import com.evolutiongaming.kafka.journal.KafkaConverters._
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.serialization.{SerializedMsg, SerializedMsgExt}
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig, CreateConsumer}
import com.evolutiongaming.skafka.producer.{CreateProducer, ProducerConfig}

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try
import scala.util.control.NonFatal

class PersistenceJournal extends AsyncWriteJournal {

  val serializedMsgExt = SerializedMsgExt(context.system)
  implicit val system = context.system
  implicit val ec = system.dispatcher

  val log = ActorLog(system, classOf[PersistenceJournal])

  lazy val journals: Journals = {

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

    val eventualJournal: EventualJournal = {
      val cassandraConfig = CassandraConfig.Default
      val cluster = CreateCluster(cassandraConfig)
      val session = cluster.connect()
      val schemaConfig = SchemaConfig.Default
      val config = EventualCassandraConfig.Default
      // TODO read only cassandra statements
      val log = ActorLog(system, EventualCassandra.getClass)
      val eventualJournal = EventualCassandra(session, schemaConfig, config, log)
      EventualJournal(eventualJournal, log)
    }

    Journals(producer, newConsumer, eventualJournal)
  }

  // TODO optimise sequence of calls asyncWriteMessages & asyncReadHighestSequenceNr for the same persistenceId

  def asyncWriteMessages(atomicWrites: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    val timestamp = Instant.now()
    val persistentReprs = for {
      atomicWrite <- atomicWrites
      persistentRepr <- atomicWrite.payload
    } yield {
      persistentRepr
    }
    if (persistentReprs.isEmpty) Future.seq
    else {
      val persistenceId = persistentReprs.head.persistenceId

      def seqNrs = persistentReprs.map(_.sequenceNr).mkString(",")

      log.debug(s"asyncWriteMessages persistenceId: $persistenceId, seqNrs: $seqNrs")

      val result = Future {
        val events = for {
          persistentRepr <- persistentReprs
        } yield {
          val (payload: AnyRef, tags) = PayloadAndTags(persistentRepr.payload)
          val serialized = serializedMsgExt.toMsg(payload)
          val persistentEvent = PersistentEvent(serialized, persistentRepr)
          val bytes = PersistentEventSerializer.toBinary(persistentEvent)
          Event(persistentRepr.sequenceNr, tags, Bytes(bytes))
        }
        val nel = Nel(events.head, events.tail.toList) // TODO is it optimal convert to list ?
        val result = journals.append(persistenceId, nel, timestamp)
        result.map(_ => Nil)(CurrentThreadExecutionContext)
      }
      result.flatMap(identity)(CurrentThreadExecutionContext)
    }
  }

  def asyncDeleteMessagesTo(persistenceId: PersistenceId, to: SeqNr): Future[Unit] = {
    val timestamp = Instant.now()
    journals.delete(persistenceId, to, timestamp)
  }

  def asyncReplayMessages(persistenceId: PersistenceId, from: SeqNr, to: SeqNr, max: Long)
    (callback: PersistentRepr => Unit): Future[Unit] = {

    journals.foldWhile(persistenceId, from, 0l) { (count, event) =>
      if (event.seqNr <= to && count < max) {
        val persistentEvent = PersistentEventSerializer.fromBinary(event.payload.value)
        val serializedMsg = SerializedMsg(persistentEvent.identifier, persistentEvent.manifest, persistentEvent.payload)
        val payload = serializedMsgExt.fromMsg(serializedMsg).get
        val seqNr = persistentEvent.seqNr
        val persistentRepr = PersistentRepr(
          payload = payload,
          sequenceNr = seqNr,
          persistenceId = persistenceId,
          manifest = persistentEvent.persistentManifest,
          writerUuid = persistentEvent.writerUuid)
        callback(persistentRepr)
        val result = count + 1
        (result, result != max)
      } else {
        (count, false)
      }
    }.unit
  }

  def asyncReadHighestSequenceNr(persistenceId: PersistenceId, from: SeqNr): Future[SeqNr] = {
    journals.lastSeqNr(persistenceId, from)
  }
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
