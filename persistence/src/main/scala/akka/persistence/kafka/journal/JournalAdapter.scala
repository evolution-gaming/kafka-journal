package akka.persistence.kafka.journal

import akka.persistence.{AtomicWrite, PersistentRepr}
import cats.effect._
import cats.implicits._
import cats.temp.par.Par
import cats.{Monad, ~>}
import com.evolutiongaming.catshelper.{FromFuture, Log, LogOf, Runtime, ToFuture}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandra
import com.evolutiongaming.kafka.journal.stream.Stream
import com.evolutiongaming.kafka.journal.util.Executors
import com.evolutiongaming.nel.Nel
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

  def of[F[_] : Concurrent : ContextShift : FromFuture : ToFuture : Par : Timer : LogOf : Runtime : RandomId : FromGFuture : MeasureDuration](
    toKey: ToKey,
    origin: Option[Origin],
    serializer: EventSerializer[cats.Id],
    config: KafkaJournalConfig,
    metrics: Metrics[F],
    log: Log[F],
    batching: Batching[F],
    metadataAndHeadersOf: MetadataAndHeadersOf[F],
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
      eventualJournal  <- EventualCassandra.of[F](config.cassandra, metrics.eventual, cassandraClusterOf)
      journal          <- journal(eventualJournal)(kafkaConsumerOf1, kafkaProducerOf1, headCacheOf1)
    } yield {
      JournalAdapter[F](journal, toKey, serializer, metadataAndHeadersOf).withBatching(batching)
    }
  }

  def apply[F[_] : Monad : Clock](
    journal: Journal[F],
    toKey: ToKey,
    serializer: EventSerializer[cats.Id],
    metadataAndHeadersOf: MetadataAndHeadersOf[F]
  ): JournalAdapter[F] = new JournalAdapter[F] {

    def write(aws: Seq[AtomicWrite]) = {
      val prs = aws.flatMap(_.payload)
      Nel.opt(prs).fold {
        List.empty[Try[Unit]].pure[F]
      } { prs =>
        val persistenceId = prs.head.persistenceId
        val key = toKey(persistenceId)
        val events = prs.map(serializer.toEvent)
        for {
          mah <- metadataAndHeadersOf(key, prs, events)
          _   <- journal.append(key, events, mah.metadata, mah.headers)
        } yield List.empty[Try[Unit]]
      }
    }

    def delete(persistenceId: PersistenceId, to: SeqNr) = {
      val key  = toKey(persistenceId)
      journal.delete(key, to).void
    }

    def replay(persistenceId: PersistenceId, range: SeqRange, max: Long)(f: PersistentRepr => F[Unit]) = {

      val key = toKey(persistenceId)

      val stream = journal
        .read(key, range.from)
        .foldMapCmdM(max) { (n, record) =>
          val event = record.event
          val seqNr = event.seqNr
          if (n > 0 && seqNr <= range.to) {
            val persistentRepr = serializer.toPersistentRepr(persistenceId, event)
            for {
              _ <- f(persistentRepr)
            } yield {
              (n - 1, Stream.Cmd.take(event))
            }
          } else {
            (n, Stream.Cmd.stop[Event]).pure[F]
          }
        }

      stream.drain
    }

    def lastSeqNr(persistenceId: PersistenceId, from: SeqNr) = {
      val key = toKey(persistenceId)
      for {
        pointer <- journal.pointer(key)
      } yield for {
        pointer <- pointer
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
    headCache: Option[HeadCache.Metrics[F]] = None,
    producer: Option[ClientId => ProducerMetrics[F]] = None,
    consumer: Option[ClientId => ConsumerMetrics[F]] = None)

  object Metrics {
    def empty[F[_]]: Metrics[F] = Metrics[F]()
  }
}