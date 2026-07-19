package com.evolution.kafka.journal.akka.persistence

import akka.persistence.{AtomicWrite, PersistentRepr}
import cats.effect.*
import cats.syntax.all.*
import cats.{Monad, MonadThrow, Parallel, ~>}
import com.evolution.kafka.journal.*
import com.evolution.kafka.journal.conversions.{ConversionMetrics, KafkaRead, KafkaWrite}
import com.evolution.kafka.journal.eventual.cassandra.EventualCassandra
import com.evolution.kafka.journal.eventual.{EventualJournal, EventualRead}
import com.evolution.kafka.journal.util.Fail
import com.evolutiongaming.catshelper.*
import com.evolutiongaming.scassandra.CassandraClusterOf
import com.evolutiongaming.scassandra.util.FromGFuture
import com.evolutiongaming.skafka.consumer.ConsumerMetrics
import com.evolutiongaming.skafka.producer.ProducerMetrics
import com.evolutiongaming.skafka.{ClientId, CommonConfig}

import scala.util.{Failure, Success, Try}

trait JournalAdapter[F[_]] {

  def write(aws: Seq[AtomicWrite]): F[List[Try[Unit]]]

  def delete(persistenceId: PersistenceId, to: DeleteTo): F[Unit]

  def lastSeqNr(persistenceId: PersistenceId, from: SeqNr): F[Option[SeqNr]]

  def replay(persistenceId: PersistenceId, range: SeqRange, max: Long)(f: PersistentRepr => F[Unit]): F[Unit]
}

object JournalAdapter {

  def make[
    F[
      _,
    ]: Async: Parallel: LogOf: RandomIdOf: FromGFuture: MeasureDuration: ToTry: FromTry: FromJsResult: Fail: JsonCodec,
    A,
  ](
    toKey: ToKey[F],
    origin: Option[Origin],
    serializer: EventSerializer[F, A],
    journalReadWrite: JournalReadWrite[F, A],
    config: KafkaJournalConfig,
    metrics: Metrics[F],
    log: Log[F],
    batching: Batching[F],
    appendMetadataOf: AppendMetadataOf[F],
    cassandraClusterOf: CassandraClusterOf[F],
  ): Resource[F, JournalAdapter[F]] = {
    makeWithJournals[F, A](
      toKey = toKey,
      origin = origin,
      serializer = serializer,
      journalReadWrite = journalReadWrite,
      config = config,
      metrics = metrics,
      log = log,
      batching = batching,
      appendMetadataOf = appendMetadataOf,
      cassandraClusterOf = cassandraClusterOf,
    ).map { case (adapter, _) => adapter }
  }

  def makeWithJournals[
    F[
      _,
    ]: Async: Parallel: LogOf: RandomIdOf: FromGFuture: MeasureDuration: ToTry: FromTry: FromJsResult: Fail: JsonCodec,
    A,
  ](
    toKey: ToKey[F],
    origin: Option[Origin],
    serializer: EventSerializer[F, A],
    journalReadWrite: JournalReadWrite[F, A],
    config: KafkaJournalConfig,
    metrics: Metrics[F],
    log: Log[F],
    batching: Batching[F],
    appendMetadataOf: AppendMetadataOf[F],
    cassandraClusterOf: CassandraClusterOf[F],
  ): Resource[F, (JournalAdapter[F], Journals[F])] = {

    def clientIdOf(config: CommonConfig) = config.clientId getOrElse "journal"

    val kafkaConsumerOf = {
      val consumerMetrics = for {
        metrics <- metrics.consumer
      } yield {
        val clientId = clientIdOf(config.journal.kafka.consumer.common)
        metrics(clientId)
      }
      KafkaConsumerOf[F](consumerMetrics)
    }

    val kafkaProducerOf = {
      val producerMetrics = for {
        metrics <- metrics.producer
      } yield {
        val clientId = clientIdOf(config.journal.kafka.producer.common)
        metrics(clientId)
      }
      KafkaProducerOf[F](producerMetrics)
    }

    def headCacheOf(
      implicit
      kafkaConsumerOf: KafkaConsumerOf[F],
    ): HeadCacheOf[F] = {
      HeadCacheOf[F](metrics.headCache)
    }

    def journal(
      eventualJournal: EventualJournal[F],
    )(implicit
      kafkaConsumerOf: KafkaConsumerOf[F],
      kafkaProducerOf: KafkaProducerOf[F],
      headCacheOf: HeadCacheOf[F],
    ): Resource[F, Journals[F]] = {
      Journals
        .make[F](
          origin = origin,
          config = config.journal,
          eventualJournal = eventualJournal,
          journalMetrics = metrics.journal,
          conversionMetrics = metrics.conversion,
          consumerPoolConfig = config.consumerPool,
          consumerPoolMetrics = metrics.consumerPool,
          callTimeThresholds = config.callTimeThresholds,
        )
        .map { _.withLogError(log) }
    }

    val headCacheOf1 = headCacheOf(kafkaConsumerOf)

    for {
      eventualJournal <- EventualCassandra.make[F](
        config.cassandra,
        origin,
        metrics.eventual,
        cassandraClusterOf,
        config.dataIntegrity,
      )
      journals <- journal(eventualJournal)(kafkaConsumerOf, kafkaProducerOf, headCacheOf1)
    } yield {
      val adapter = JournalAdapter[F, A](journals, toKey, serializer, journalReadWrite, appendMetadataOf)
        .withBatching(batching)
      (adapter, journals)
    }
  }

  def apply[F[_]: MonadThrow, A](
    journals: Journals[F],
    toKey: ToKey[F],
    serializer: EventSerializer[F, A],
    journalReadWrite: JournalReadWrite[F, A],
    appendMetadataOf: AppendMetadataOf[F],
  ): JournalAdapter[F] = {

    implicit val kafkaRead: KafkaRead[F, A] = journalReadWrite.kafkaRead
    implicit val kafkaWrite: KafkaWrite[F, A] = journalReadWrite.kafkaWrite
    implicit val eventualRead: EventualRead[F, A] = journalReadWrite.eventualRead

    new JournalAdapter[F] {

      def write(aws: Seq[AtomicWrite]): F[List[Try[Unit]]] = {
        aws.flatMap(_.payload).headOption.foldMapM { head =>
          val persistenceId = head.persistenceId
          for {
            key <- toKey(persistenceId)
            // Serialize each atomic write independently so that a serialization failure is surfaced as a
            // per-write rejection (`Failure`, routed to `onPersistRejected`) rather than by failing `F` - which
            // Akka/Pekko treats as a store failure that stops/restarts the persistent actor, taking down the
            // unrelated valid writes in the same batch.
            results <- aws.toList.traverse { aw =>
              aw.payload.toList.toNel.traverse { prs =>
                prs.traverse(serializer.toEvent).map { events => (prs, events) }
              }.attempt
            }
            accepted = results.collect { case Right(Some(prsAndEvents)) => prsAndEvents }
            _ <- accepted.toNel.traverse_ { accepted =>
              val prs = accepted.flatMap { case (prs, _) => prs }
              val events = accepted.flatMap { case (_, events) => events }
              for {
                metadata <- appendMetadataOf(key, prs, events)
                _ <- journals(key).append(events, metadata.metadata, metadata.headers)
              } yield {}
            }
          } yield {
            if (results.forall(_.isRight)) List.empty[Try[Unit]]
            else
              results.map {
                case Right(_) => Success(())
                case Left(error) => Failure(error)
              }
          }
        }
      }

      def delete(persistenceId: PersistenceId, to: DeleteTo): F[Unit] = {
        for {
          key <- toKey(persistenceId)
          _ <- journals(key).delete(to)
        } yield {}
      }

      def replay(persistenceId: PersistenceId, range: SeqRange, max: Long)(f: PersistentRepr => F[Unit]): F[Unit] = {

        def replay(key: Key): F[Unit] = {
          journals(key)
            .read[A](range.from)
            .foldWhileM(max) { (max, record) =>
              val event = record.event
              val seqNr = event.seqNr
              if (max > 0 && seqNr <= range.to) {
                for {
                  persistentRepr <- serializer.toPersistentRepr(persistenceId, event)
                  _ <- f(persistentRepr)
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
          key <- toKey(persistenceId)
          result <- replay(key)
        } yield result
      }

      def lastSeqNr(persistenceId: PersistenceId, from: SeqNr): F[Option[SeqNr]] = {
        for {
          key <- toKey(persistenceId)
          pointer <- journals(key).pointer
        } yield
          for {
            pointer <- pointer
            if pointer >= from
          } yield pointer
      }
    }
  }

  implicit class JournalAdapterOps[F[_]](val self: JournalAdapter[F]) extends AnyVal {

    def mapK[G[_]](fg: F ~> G, gf: G ~> F): JournalAdapter[G] = new JournalAdapter[G] {

      def write(aws: Seq[AtomicWrite]): G[List[Try[Unit]]] = fg(self.write(aws))

      def delete(persistenceId: PersistenceId, to: DeleteTo): G[Unit] = fg(self.delete(persistenceId, to))

      def lastSeqNr(persistenceId: PersistenceId, from: SeqNr): G[Option[SeqNr]] =
        fg(self.lastSeqNr(persistenceId, from))

      def replay(persistenceId: PersistenceId, range: SeqRange, max: Long)(f: PersistentRepr => G[Unit]): G[Unit] = {
        fg(self.replay(persistenceId, range, max)(a => gf(f(a))))
      }
    }

    def withBatching(
      batching: Batching[F],
    )(implicit
      F: Monad[F],
    ): JournalAdapter[F] = new JournalAdapter[F] {

      def write(aws: Seq[AtomicWrite]): F[List[Try[Unit]]] = {
        if (aws.size <= 1) self.write(aws)
        else
          for {
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

      def delete(persistenceId: PersistenceId, to: DeleteTo): F[Unit] = self.delete(persistenceId, to)

      def lastSeqNr(persistenceId: PersistenceId, from: SeqNr): F[Option[SeqNr]] = self.lastSeqNr(persistenceId, from)

      def replay(persistenceId: PersistenceId, range: SeqRange, max: Long)(f: PersistentRepr => F[Unit]): F[Unit] = {
        self.replay(persistenceId, range, max)(f)
      }
    }
  }

  final case class Metrics[F[_]](
    journal: Option[JournalMetrics[F]] = None,
    eventual: Option[EventualJournal.Metrics[F]] = None,
    headCache: Option[HeadCacheMetrics[F]] = None,
    producer: Option[ClientId => ProducerMetrics[F]] = None,
    consumer: Option[ClientId => ConsumerMetrics[F]] = None,
    conversion: Option[ConversionMetrics[F]] = None,
    consumerPool: Option[ConsumerPoolMetrics[F]] = None,
  )

  object Metrics {
    def empty[F[_]]: Metrics[F] = Metrics[F]()
  }
}
