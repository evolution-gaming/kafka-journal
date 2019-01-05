package akka.persistence.kafka.journal

import akka.actor.ActorSystem
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{AtomicWrite, PersistentRepr}
import cats.effect.IO
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal.AsyncHelper._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandra
import com.evolutiongaming.kafka.journal.util.FromFuture
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.scassandra.CreateCluster
import com.evolutiongaming.skafka.ClientId
import com.evolutiongaming.skafka.consumer.Consumer
import com.evolutiongaming.skafka.producer.Producer
import com.typesafe.config.Config

import scala.collection.immutable.Seq
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.Try
import scala.util.control.NonFatal

class KafkaJournal(config: Config) extends AsyncWriteJournal {
  import KafkaJournal._

  implicit val system: ActorSystem = context.system
  implicit val ec: ExecutionContextExecutor = context.dispatcher

  val log: ActorLog = ActorLog(system, classOf[KafkaJournal])

  val adapter: JournalAdapter = adapterOf(
    toKey(),
    origin(),
    serializer(),
    kafkaJournalConfig(),
    metrics())

  def toKey(): ToKey = ToKey(config)

  def kafkaJournalConfig() = KafkaJournalConfig(config)

  def origin(): Option[Origin] = Some {
    Origin.HostName orElse Origin.AkkaHost(system) getOrElse Origin.AkkaName(system)
  }

  def serializer(): EventSerializer = EventSerializer(system)

  def metrics(): Metrics = Metrics.Empty

  def adapterOf(
    toKey: ToKey,
    origin: Option[Origin],
    serializer: EventSerializer,
    config: KafkaJournalConfig,
    metrics: Metrics): JournalAdapter = {

    log.debug(s"Config: $config")

    val ecBlocking = system.dispatchers.lookup(config.blockingDispatcher)

    val producer = {
      val producerConfig = config.journal.producer
      val producer = Producer(producerConfig, ecBlocking)
      metrics.producer.fold(producer) { metrics =>
        val clientId = producerConfig.common.clientId getOrElse "journal"
        Producer(producer, metrics(clientId))
      }
    }

    system.registerOnTermination {
      val future = for {
        _ <- producer.flush()
        _ <- producer.close(config.stopTimeout)
      } yield ()
      try Await.result(future, config.stopTimeout) catch {
        case NonFatal(failure) => log.error(s"failed to shutdown producer $failure", failure)
      }
    }

    val topicConsumer = {
      implicit val cs = IO.contextShift(ec)
      implicit val fromFuture = FromFuture.lift[IO]
      val consumerConfig = config.journal.consumer
      val consumerMetrics = for {
        metrics <- metrics.consumer
      } yield {
        val clientId = consumerConfig.common.clientId getOrElse "journal"
        metrics(clientId)
      }
      TopicConsumer[IO](consumerConfig, ecBlocking, metrics = consumerMetrics)
    }

    val eventualJournal: EventualJournal[Async] = {
      val cassandraConfig = config.cassandra
      val cluster = CreateCluster(cassandraConfig.client)
      implicit val session = Await.result(cluster.connect(), config.connectTimeout)
      system.registerOnTermination {
        val result = for {
          _ <- session.close()
          _ <- cluster.close()
        } yield {}
        try {
          Await.result(result, config.stopTimeout)
        } catch {
          case NonFatal(failure) => log.error(s"failed to shutdown cassandra $failure", failure)
        }
      }

      {
        val actorLog = ActorLog(system, EventualJournal.getClass)
        implicit val log = Log.async(actorLog)
        val journal = {
          val journal = EventualCassandra(cassandraConfig, actorLog, origin)
          EventualJournal(journal)
        }
        metrics.eventual.fold(journal) { EventualJournal(journal, _) }
      }
    }

    val headCache = {
      if (config.headCache) {
        HeadCacheAsync(config.journal.consumer, eventualJournal, ecBlocking)
      } else {
        HeadCache.empty[Async]
      }
    }

    system.registerOnTermination {
      try {
        Await.result(headCache.close.future, config.stopTimeout)
      } catch {
        case NonFatal(failure) => log.error(s"failed to shutdown headCache $failure", failure)
      }
    }

    val journal = {
      val journal = Journal(
        producer = producer,
        origin = origin,
        topicConsumer = topicConsumer,
        eventual = eventualJournal,
        pollTimeout = config.journal.pollTimeout,
        headCache = headCache)

      metrics.journal.fold(journal) { Journal(journal, _) }
    }
    JournalAdapter(log, toKey, journal, serializer)
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

object KafkaJournal {

  final case class Metrics(
    journal: Option[Journal.Metrics[Async]] = None,
    eventual: Option[EventualJournal.Metrics[Async]] = None,
    producer: Option[ClientId => Producer.Metrics] = None,
    consumer: Option[ClientId => Consumer.Metrics] = None)

  object Metrics {
    val Empty: Metrics = Metrics()
  }
}