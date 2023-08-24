package akka.persistence.kafka.journal

import akka.persistence.{AtomicWrite, PersistentRepr}
import cats.effect._
import cats.syntax.all._
import cats.{Monad, Parallel, ~>}
import com.evolutiongaming.catshelper._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.conversions.ConversionMetrics
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandra
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.scassandra.CassandraClusterOf
import com.evolutiongaming.scassandra.util.FromGFuture
import com.evolutiongaming.skafka.consumer.ConsumerMetrics
import com.evolutiongaming.skafka.producer.ProducerMetrics
import com.evolutiongaming.skafka.{ClientId, CommonConfig}

import scala.util.Try

trait JournalAdapter[F[_]] {

  def write(aws: Seq[AtomicWrite]): F[List[Try[Unit]]]

  def delete(persistenceId: PersistenceId, to: DeleteTo): F[Unit]

  def lastSeqNr(persistenceId: PersistenceId, from: SeqNr): F[Option[SeqNr]]

  def replay(persistenceId: PersistenceId, range: SeqRange, max: Long)(f: PersistentRepr => F[Unit]): F[Unit]
}

object JournalAdapter {

  def of[
    F[_]: Async: ToFuture: Parallel: LogOf: RandomIdOf:
    FromGFuture: MeasureDuration: ToTry: FromTry: FromJsResult: Fail: JsonCodec,
    A
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
    cassandraClusterOf: CassandraClusterOf[F]
  ): Resource[F, JournalAdapter[F]] = {

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

    def headCacheOf(implicit kafkaConsumerOf: KafkaConsumerOf[F]): HeadCacheOf[F] = {
      HeadCacheOf[F](metrics.headCache)
    }

    def journal(
      eventualJournal: EventualJournal[F])(implicit
      kafkaConsumerOf: KafkaConsumerOf[F],
      kafkaProducerOf: KafkaProducerOf[F],
      headCacheOf: HeadCacheOf[F]) = {

      val journal = config.consumerPool match {
        case Some(consumerPoolConfig) =>
          Journals.make[F](
            origin = origin,
            config = config.journal,
            eventualJournal = eventualJournal,
            journalMetrics = metrics.journal,
            conversionMetrics = metrics.conversion,
            consumerPoolConfig = consumerPoolConfig,
            consumerPoolMetrics = metrics.consumerPool,
            callTimeThresholds = config.callTimeThresholds
          )
        case None =>
          Journals.of[F](
            origin = origin,
            config = config.journal,
            eventualJournal = eventualJournal,
            journalMetrics = metrics.journal,
            conversionMetrics = metrics.conversion,
            callTimeThresholds = config.callTimeThresholds
          )
      }

      for {
        journal <- journal
      } yield {
        journal.withLogError(log)
      }
    }

    val headCacheOf1 = headCacheOf(kafkaConsumerOf)

    for {
      eventualJournal  <- EventualCassandra.of[F](config.cassandra, origin, metrics.eventual, cassandraClusterOf)
      journal          <- journal(eventualJournal)(kafkaConsumerOf, kafkaProducerOf, headCacheOf1)
    } yield {
      JournalAdapter[F, A](journal, toKey, serializer, journalReadWrite, appendMetadataOf).withBatching(batching)
    }
  }

  def apply[F[_]: Monad, A](
    journals: Journals[F],
    toKey: ToKey[F],
    serializer: EventSerializer[F, A],
    journalReadWrite: JournalReadWrite[F, A],
    appendMetadataOf: AppendMetadataOf[F]
  ): JournalAdapter[F] = {

    implicit val kafkaRead = journalReadWrite.kafkaRead
    implicit val kafkaWrite = journalReadWrite.kafkaWrite
    implicit val eventualRead = journalReadWrite.eventualRead

    new JournalAdapter[F] {

      def write(aws: Seq[AtomicWrite]) = {
        val prs = aws.flatMap(_.payload)
        prs.toList.toNel.foldMapM { prs =>
          val persistenceId = prs.head.persistenceId
          for {
            key      <- toKey(persistenceId)
            events   <- prs.traverse(serializer.toEvent)
            metadata <- appendMetadataOf(key, prs, events)
            journal   = journals(key)
            _        <- journal.append(events, metadata.metadata, metadata.headers)
          } yield {
            List.empty[Try[Unit]]
          }
        }
      }

      def delete(persistenceId: PersistenceId, to: DeleteTo) = {
        for {
          key <- toKey(persistenceId)
          _   <- journals(key).delete(to)
        } yield {}
      }

      def replay(persistenceId: PersistenceId, range: SeqRange, max: Long)(f: PersistentRepr => F[Unit]) = {

        def replay(key: Key): F[Unit] = {
          journals(key)
            .read[A](range.from)
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
          pointer <- journals(key).pointer
        } yield for {
          pointer    <- pointer
          if pointer >= from
        } yield pointer
      }
    }
  }

  implicit class JournalAdapterOps[F[_]](val self: JournalAdapter[F]) extends AnyVal {

    def mapK[G[_]](fg: F ~> G, gf: G ~> F): JournalAdapter[G] = new JournalAdapter[G] {

      def write(aws: Seq[AtomicWrite]) = fg(self.write(aws))

      def delete(persistenceId: PersistenceId, to: DeleteTo) = fg(self.delete(persistenceId, to))

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

      def delete(persistenceId: PersistenceId, to: DeleteTo) = self.delete(persistenceId, to)

      def lastSeqNr(persistenceId: PersistenceId, from: SeqNr) = self.lastSeqNr(persistenceId, from)

      def replay(persistenceId: PersistenceId, range: SeqRange, max: Long)(f: PersistentRepr => F[Unit]) = {
        self.replay(persistenceId, range, max)(f)
      }
    }
  }


  final case class Metrics[F[_]](
    journal: Option[JournalMetrics[F]] = none,
    eventual: Option[EventualJournal.Metrics[F]] = none,
    headCache: Option[HeadCacheMetrics[F]] = none,
    producer: Option[ClientId => ProducerMetrics[F]] = none,
    consumer: Option[ClientId => ConsumerMetrics[F]] = none,
    conversion: Option[ConversionMetrics[F]] = none,
    consumerPool: Option[ConsumerPoolMetrics[F]] = none,
  )

  object Metrics {
    def empty[F[_]]: Metrics[F] = Metrics[F]()
  }
}
