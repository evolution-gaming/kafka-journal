package akka.persistence.kafka.journal

import akka.actor.ActorSystem
import akka.persistence.{AtomicWrite, PersistentRepr}
import cats.effect._
import cats.implicits._
import cats.{Monad, ~>}
import com.evolutiongaming.kafka.journal.FoldWhile.{Fold, _}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.eventual.cassandra.{CassandraCluster, CassandraSession, EventualCassandra}
import com.evolutiongaming.kafka.journal.util.ClockHelper._
import com.evolutiongaming.kafka.journal.util.{FromFuture, Par, ToFuture}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.ClientId
import com.evolutiongaming.skafka.consumer.Consumer
import com.evolutiongaming.skafka.producer.Producer

import scala.collection.immutable.Seq
import scala.concurrent.ExecutionContext
import scala.util.Try

trait JournalAdapter[F[_]] {

  def write(messages: Seq[AtomicWrite]): F[List[Try[Unit]]]

  def delete(persistenceId: String, to: SeqNr): F[Unit]

  def lastSeqNr(persistenceId: String, from: SeqNr): F[Option[SeqNr]]

  def replay(persistenceId: String, range: SeqRange, max: Long)(f: PersistentRepr => Unit): F[Unit]
}

object JournalAdapter {

  def of[F[_] : Concurrent : ContextShift : FromFuture : ToFuture : Par : Timer : LogOf](
    toKey: ToKey,
    origin: Option[Origin],
    serializer: EventSerializer,
    config: KafkaJournalConfig,
    metrics: Metrics[F],
    actorLog: ActorLog)(implicit
    system: ActorSystem): Resource[F, JournalAdapter[F]] = {

    val blocking = Sync[F].delay {
      system.dispatchers.lookup(config.blockingDispatcher)
    }

    def kafkaProducer(blocking: ExecutionContext) = {
      val producerConfig = config.journal.producer
      val producerMetrics = for {
        metrics <- metrics.producer
      } yield {
        val clientId = producerConfig.common.clientId getOrElse "journal"
        metrics(clientId)
      }
      KafkaProducer.of[F](producerConfig, blocking, producerMetrics)
    }

    def eventualJournalOf(implicit cassandraSession: CassandraSession[F]) = {
      for {
        journal <- EventualCassandra.of[F](config.cassandra, origin)
      } yield {
        metrics.eventual.fold(journal) { metrics => EventualJournal[F](journal, metrics) }
      }
    }

    def headCache(eventualJournal: EventualJournal[F], blocking: ExecutionContext) = {
      val result = for {
        headCache <- {
          if (config.headCache) {
            HeadCache.of[F](config.journal.consumer, eventualJournal, blocking)
          } else {
            HeadCache.empty[F].pure[F]
          }
        }
      } yield {
        (headCache, headCache.close)
      }
      Resource(result)
    }


    def journal(
      eventualJournal: EventualJournal[F],
      kafkaProducer: KafkaProducer[F],
      headCache: HeadCache[F],
      blocking: ExecutionContext) = {

      val topicConsumer = {
        val consumerConfig = config.journal.consumer
        val consumerMetrics = for {
          metrics <- metrics.consumer
        } yield {
          val clientId = consumerConfig.common.clientId getOrElse "journal"
          metrics(clientId)
        }
        TopicConsumer[F](consumerConfig, blocking, metrics = consumerMetrics)
      }

      for {
        log <- LogOf[F].apply(Journal.getClass)
      } yield {
        implicit val log1 = log
        val journal = Journal[F](
          kafkaProducer = kafkaProducer,
          origin = origin,
          topicConsumer = topicConsumer,
          eventualJournal = eventualJournal,
          pollTimeout = config.journal.pollTimeout,
          headCache = headCache)

        metrics.journal.fold(journal) { metrics => journal.withMetrics(metrics) }
      }
    }

    for {
      cassandraCluster <- CassandraCluster.of[F](config.cassandra.client, config.cassandra.retries)
      cassandraSession <- cassandraCluster.session
      blocking         <- Resource.liftF(blocking)
      kafkaProducer    <- kafkaProducer(blocking)
      eventualJournal  <- Resource.liftF(eventualJournalOf(cassandraSession))
      headCache        <- headCache(eventualJournal, blocking)
      journal          <- Resource.liftF(journal(eventualJournal, kafkaProducer, headCache, blocking))
    } yield {
      implicit val log = Log[F](actorLog)
      JournalAdapter[F](journal, toKey, serializer)
    }
  }

  def apply[F[_] : Monad : Clock : Log](
    journal: Journal[F],
    toKey: ToKey,
    serializer: EventSerializer): JournalAdapter[F] = {

    new JournalAdapter[F] {

      def write(aws: Seq[AtomicWrite]) = {
        for {
          timestamp <- Clock[F].instant
          prs        = for { aw <- aws; pr <- aw.payload } yield pr
          result    <- Nel.opt(prs).fold(List.empty[Try[Unit]].pure[F]) { prs =>
            val persistenceId = prs.head.persistenceId
            val key = toKey(persistenceId)
            for {
              _      <- Log[F].debug {
                val first = prs.head.sequenceNr
                val last = prs.last.sequenceNr
                val seqNr = if (first == last) s"seqNr: $first" else s"seqNrs: $first..$last"
                s"$persistenceId write, $seqNr"
              }
              events  = prs.map(serializer.toEvent)
              _      <- journal.append(key, events, timestamp)
            } yield List.empty[Try[Unit]]
          }
        } yield result
      }

      def delete(persistenceId: PersistenceId, to: SeqNr) = {
        for {
          timestamp <- Clock[F].instant
          _         <- Log[F].debug(s"$persistenceId delete, to: $to")
          key        = toKey(persistenceId)
          _         <- journal.delete(key, to, timestamp)
        } yield {}
      }

      def replay(persistenceId: PersistenceId, range: SeqRange, max: Long)
        (callback: PersistentRepr => Unit) = {

        val key = toKey(persistenceId)

        val fold: Fold[Long, Event] = (count, event) => {
          val seqNr = event.seqNr
          if (seqNr <= range.to && count < max) {
            val persistentRepr = serializer.toPersistentRepr(persistenceId, event)
            callback(persistentRepr)
            val countNew = count + 1
            countNew switch countNew != max
          } else {
            count.stop
          }
        }

        for {
          _ <- Log[F].debug(s"$persistenceId replay, range: $range")
          _ <- journal.read(key, range.from, 0l)(fold)
        } yield {}
      }

      def lastSeqNr(persistenceId: PersistenceId, from: SeqNr) = {
        val key = toKey(persistenceId)
        for {
          _       <- Log[F].debug(s"$persistenceId lastSeqNr, from: $from")
          pointer <- journal.pointer(key)
        } yield for {
          pointer <- pointer
          if pointer >= from
        } yield pointer
      }
    }
  }


  implicit class JournalAdapterOps[F[_]](val self: JournalAdapter[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): JournalAdapter[G] = new JournalAdapter[G] {

      def write(messages: Seq[AtomicWrite]) = f(self.write(messages))

      def delete(persistenceId: String, to: SeqNr) = f(self.delete(persistenceId, to))

      def lastSeqNr(persistenceId: String, from: SeqNr) = f(self.lastSeqNr(persistenceId, from))

      def replay(persistenceId: String, range: SeqRange, max: Long)(f1: PersistentRepr => Unit) = {
        f(self.replay(persistenceId, range, max)(f1))
      }
    }
  }


  final case class Metrics[F[_]](
    journal: Option[Journal.Metrics[F]] = None,
    eventual: Option[EventualJournal.Metrics[F]] = None,
    producer: Option[ClientId => Producer.Metrics] = None,
    consumer: Option[ClientId => Consumer.Metrics] = None)

  object Metrics {
    def empty[F[_]]: Metrics[F] = Metrics[F]()
  }
}