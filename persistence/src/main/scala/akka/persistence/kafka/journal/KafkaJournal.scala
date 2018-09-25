package akka.persistence.kafka.journal

import java.util.UUID

import akka.actor.ActorSystem
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{AtomicWrite, PersistentRepr}
import com.evolutiongaming.cassandra.CreateCluster
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.config.ConfigHelper._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.eventual.cassandra.{EventualCassandra, EventualCassandraConfig}
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.skafka.consumer.{Consumer, ConsumerConfig}
import com.evolutiongaming.skafka.producer.{Producer, ProducerConfig}
import com.typesafe.config.Config

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.Try
import scala.util.control.NonFatal

class KafkaJournal(config: Config) extends AsyncWriteJournal {

  implicit val system: ActorSystem = context.system
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  val log: ActorLog = ActorLog(system, classOf[KafkaJournal])

  val adapter: JournalsAdapter = adapterOf(toKey(), serializer())

  def toKey(): ToKey = ToKey(config)

  def kafkaConfig(name: String): Config = {
    val common = config.getConfig("kafka")
    common.getConfig(name) withFallback common
  }

  def producerConfig() = ProducerConfig(kafkaConfig("producer"))

  def consumerConfig() = ConsumerConfig(kafkaConfig("consumer"))

  def cassandraConfig(): EventualCassandraConfig = {
    config.getOpt[Config]("cassandra") match {
      case Some(config) => EventualCassandraConfig(config)
      case None         => EventualCassandraConfig.Default
    }
  }

  def origin: Option[Origin] = Some {
    Origin.HostName orElse Origin.AkkaHost(system) getOrElse Origin.AkkaName(system)
  }

  def serializer(): EventSerializer = EventSerializer(system)

  def eventualJournalMetrics: Option[EventualJournal.Metrics[Async]] = None

  def journalMetrics: Option[Journal.Metrics[Async]] = None

  def adapterOf(toKey: ToKey, serializer: EventSerializer): JournalsAdapter = {

    val producerConfig = this.producerConfig()
    log.debug(s"Producer config: $producerConfig")

    val ecBlocking = system.dispatchers.lookup("evolutiongaming.kafka-journal.persistence.journal.blocking-dispatcher")

    val producer = Producer(producerConfig, ecBlocking)

    val closeTimeout = 10.seconds // TODO from config
    val connectTimeout = 5.seconds // TODO from config

    system.registerOnTermination {
      val future = for {
        _ <- producer.flush()
        _ <- producer.close(closeTimeout)
      } yield ()
      try Await.result(future, closeTimeout) catch {
        case NonFatal(failure) => log.error(s"failed to shutdown producer $failure", failure)
      }
    }

    val consumerConfig = this.consumerConfig()
    log.debug(s"Consumer config: $consumerConfig")

    val consumerOf = (topic: Topic) => {
      val uuid = UUID.randomUUID()
      val prefix = consumerConfig.groupId getOrElse "journal"
      val groupId = s"$prefix-$topic-$uuid"
      val configFixed = consumerConfig.copy(groupId = Some(groupId))
      Consumer[Id, Bytes](configFixed, ecBlocking)
    }

    val eventualJournal: EventualJournal = {
      val config = cassandraConfig()
      val cluster = CreateCluster(config.client)
      val session = Await.result(cluster.connect(), connectTimeout) // TODO handle this properly
      system.registerOnTermination {
        val result = for {
          _ <- session.close()
          _ <- cluster.close()
        } yield {}
        try {
          Await.result(result, closeTimeout)
        } catch {
          case NonFatal(failure) => log.error(s"failed to shutdown cassandra $failure", failure)
        }
      }
      // TODO read only cassandra statements

      {
        val log = ActorLog(system, EventualCassandra.getClass)
        val journal = {
          val journal = EventualCassandra(session, config)
          EventualJournal(journal, log)
        }
        eventualJournalMetrics.fold(journal) { metrics => EventualJournal(journal, metrics) }
      }
    }

    val journal = {
      val journal = Journal(producer, origin, consumerOf, eventualJournal)
      journalMetrics.fold(journal) { metrics => Journal(journal, metrics) }
    }
    JournalsAdapter(log, toKey, journal, serializer)
  }

  // TODO optimise concurrent calls asyncReplayMessages & asyncReadHighestSequenceNr for the same persistenceId
  def asyncWriteMessages(atomicWrites: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    adapter.write(atomicWrites)
  }

  def asyncDeleteMessagesTo(persistenceId: PersistenceId, to: Long): Future[Unit] = {
    SeqNr.opt(to) match {
      case Some(to) => adapter.delete(persistenceId, to)
      case None     => Future.unit
    }
  }

  def asyncReplayMessages(persistenceId: PersistenceId, from: Long, to: Long, max: Long)
    (f: PersistentRepr => Unit): Future[Unit] = {

    val seqNrFrom = SeqNr(from, SeqNr.Min)
    val seqNrTo = SeqNr(to, SeqNr.Max)
    val range = SeqRange(seqNrFrom, seqNrTo)
    adapter.replay(persistenceId, range, max)(f)
  }

  def asyncReadHighestSequenceNr(persistenceId: PersistenceId, from: Long): Future[Long] = {
    val seqNr = SeqNr(from, SeqNr.Min)
    for {
      seqNr <- adapter.lastSeqNr(persistenceId, seqNr)
    } yield seqNr match {
      case Some(seqNr) => seqNr.value
      case None        => from
    }
  }
}