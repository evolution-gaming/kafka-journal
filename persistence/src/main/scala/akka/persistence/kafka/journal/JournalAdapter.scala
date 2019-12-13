package akka.persistence.kafka.journal

import akka.persistence.{AtomicWrite, PersistentRepr}
import cats.effect._
import cats.implicits._
import cats.{Monad, Parallel, ~>}
import com.evolutiongaming.catshelper.{FromFuture, FromTry, Log, LogOf, Runtime, ToFuture, ToTry}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandra
import com.evolutiongaming.kafka.journal.util.Executors
import com.evolutiongaming.scassandra.CassandraClusterOf
import com.evolutiongaming.scassandra.util.FromGFuture
import com.evolutiongaming.skafka.consumer.ConsumerMetrics
import com.evolutiongaming.skafka.producer.ProducerMetrics
import com.evolutiongaming.skafka.{ClientId, CommonConfig}
import com.evolutiongaming.smetrics.MeasureDuration

import scala.collection.immutable.Seq
import scala.concurrent.ExecutionContext
import scala.util.Try

trait JournalAdapter[F[_]] {

  def write(aws: Seq[AtomicWrite]): F[List[Try[Unit]]]

  def delete(persistenceId: PersistenceId, to: SeqNr): F[Unit]

  def lastSeqNr(persistenceId: PersistenceId, from: SeqNr): F[Option[SeqNr]]

  def replay(persistenceId: PersistenceId, range: SeqRange, max: Long)(f: PersistentRepr => F[Unit]): F[Unit]
}

object JournalAdapter {

  def of[F[_] : ConcurrentEffect : ContextShift : FromFuture : ToFuture : Parallel : Timer : LogOf : Runtime : RandomIdOf : FromGFuture : MeasureDuration : ToTry : FromTry : FromAttempt : FromJsResult](
    toKey: ToKey[F],
    origin: Option[Origin],
    serializer: EventSerializer[F],
    config: KafkaJournalConfig,
    metrics: Metrics[F],
    log: Log[F],
    batching: Batching[F],
    appendMetadataOf: AppendMetadataOf[F],
    cassandraClusterOf: CassandraClusterOf[F]
  ): Resource[F, JournalAdapter[F]] = {

    def clientIdOf(config: CommonConfig) = config.clientId getOrElse "journal"

    def kafkaConsumerOf(blocking: ExecutionContext) = {
      val consumerMetrics = for {
        metrics <- metrics.consumer
      } yield {
        val clientId = clientIdOf(config.journal.consumer.common)
        metrics(clientId)
      }
      KafkaConsumerOf[F](blocking, consumerMetrics)
    }

    def kafkaProducerOf(blocking: ExecutionContext) = {
      val producerMetrics = for {
        metrics <- metrics.producer
      } yield {
        val clientId = clientIdOf(config.journal.producer.common)
        metrics(clientId)
      }
      KafkaProducerOf[F](blocking, producerMetrics)
    }

    def headCacheOf(implicit kafkaConsumerOf: KafkaConsumerOf[F]): HeadCacheOf[F] = {
      HeadCacheOf[F](metrics.headCache)
    }

    def journal(
      eventualJournal: EventualJournal[F])(implicit
      kafkaConsumerOf: KafkaConsumerOf[F],
      kafkaProducerOf: KafkaProducerOf[F],
      headCacheOf: HeadCacheOf[F]) = {

      for {
        journal <- Journal.of[F](
          origin = origin,
          config = config.journal,
          eventualJournal = eventualJournal,
          metrics = metrics.journal,
          callTimeThresholds = config.callTimeThresholds)
      } yield {
        journal.withLogError(log)
      }
    }

    for {
      blocking         <- Executors.blocking[F]("kafka-journal-blocking")
      kafkaProducerOf1  = kafkaProducerOf(blocking)
      kafkaConsumerOf1  = kafkaConsumerOf(blocking)
      headCacheOf1      = headCacheOf(kafkaConsumerOf1)
      eventualJournal  <- EventualCassandra.of[F](config.cassandra, origin, metrics.eventual, cassandraClusterOf)
      journal          <- journal(eventualJournal)(kafkaConsumerOf1, kafkaProducerOf1, headCacheOf1)
    } yield {
      JournalAdapter[F](journal, toKey, serializer, appendMetadataOf).withBatching(batching)
    }
  }

  def apply[F[_] : Monad : Clock](
    journal: Journal[F],
    toKey: ToKey[F],
    serializer: EventSerializer[F],
    appendMetadataOf: AppendMetadataOf[F]
  ): JournalAdapter[F] = new JournalAdapter[F] {

    def write(aws: Seq[AtomicWrite]) = {
      val prs = aws.flatMap(_.payload)
      prs.toList.toNel.foldMapM { prs =>
        val persistenceId = prs.head.persistenceId
        for {
          key      <- toKey(persistenceId)
          events   <- prs.traverse(serializer.toEvent)
          metadata <- appendMetadataOf(key, prs, events)
          _        <- journal.append(key, events, metadata.expireAfter, metadata.metadata, metadata.headers) /*TODO expiry: pass AppendMetadata directly?*/
        } yield {
          List.empty[Try[Unit]]
        }
      }
    }

    def delete(persistenceId: PersistenceId, to: SeqNr) = {
      for {
        key <- toKey(persistenceId)
        _   <- journal.delete(key, to)
      } yield {}
    }

    def replay(persistenceId: PersistenceId, range: SeqRange, max: Long)(f: PersistentRepr => F[Unit]) = {

      def replay(key: Key): F[Unit] = {
        journal
          .read(key, range.from)
          .foldWhileM(max) { (max, record) =>
            val event = record.event
            val seqNr = event.seqNr
            if (max > 0 && seqNr <= range.to) {
              for {
                persistentRepr <- serializer.toPersistentRepr(persistenceId, event)
                _              <- f(persistentRepr)
              } yield {
                if (max === 1) ().asRight[Long]
                else (max - 1).asLeft[Unit]
              }
            } else {
              ().asRight[Long].pure[F]
            }
          }
          .void
      }

      for {
        key    <- toKey(persistenceId)
        result <- replay(key)
      } yield result
    }

    def lastSeqNr(persistenceId: PersistenceId, from: SeqNr) = {
      for {
        key     <- toKey(persistenceId)
        pointer <- journal.pointer(key)
      } yield for {
        pointer    <- pointer
        if pointer >= from
      } yield pointer
    }
  }


  implicit class JournalAdapterOps[F[_]](val self: JournalAdapter[F]) extends AnyVal {

    def mapK[G[_]](fg: F ~> G, gf: G ~> F): JournalAdapter[G] = new JournalAdapter[G] {

      def write(aws: Seq[AtomicWrite]) = fg(self.write(aws))

      def delete(persistenceId: PersistenceId, to: SeqNr) = fg(self.delete(persistenceId, to))

      def lastSeqNr(persistenceId: PersistenceId, from: SeqNr) = fg(self.lastSeqNr(persistenceId, from))

      def replay(persistenceId: PersistenceId, range: SeqRange, max: Long)(f: PersistentRepr => G[Unit]) = {
        fg(self.replay(persistenceId, range, max)(a => gf(f(a))))
      }
    }


    def withBatching(batching: Batching[F])(implicit F : Monad[F]): JournalAdapter[F] = new JournalAdapter[F] {

      def write(aws: Seq[AtomicWrite]) = {
        if (aws.size <= 1) self.write(aws)
        else for {
          batches <- batching(aws.toList)
          results <- batches.foldLeftM(List.empty[List[Try[Unit]]]) { (results, group) =>
            for {
              result <- self.write(group)
            } yield {
              result :: results
            }
          }
        } yield {
          results.reverse.flatten
        }
      }

      def delete(persistenceId: PersistenceId, to: SeqNr) = self.delete(persistenceId, to)

      def lastSeqNr(persistenceId: PersistenceId, from: SeqNr) = self.lastSeqNr(persistenceId, from)

      def replay(persistenceId: PersistenceId, range: SeqRange, max: Long)(f: PersistentRepr => F[Unit]) = {
        self.replay(persistenceId, range, max)(f)
      }
    }
  }


  final case class Metrics[F[_]](
    journal: Option[Journal.Metrics[F]] = None,
    eventual: Option[EventualJournal.Metrics[F]] = None,
    headCache: Option[HeadCacheMetrics[F]] = None,
    producer: Option[ClientId => ProducerMetrics[F]] = None,
    consumer: Option[ClientId => ConsumerMetrics[F]] = None)

  object Metrics {
    def empty[F[_]]: Metrics[F] = Metrics[F]()
  }
}