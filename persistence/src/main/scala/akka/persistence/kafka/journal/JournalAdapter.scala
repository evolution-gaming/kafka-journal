package akka.persistence.kafka.journal

import akka.persistence.{AtomicWrite, PersistentRepr}
import cats.effect._
import cats.implicits._
import cats.{Monad, ~>}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandra
import com.evolutiongaming.kafka.journal.stream.Stream
import com.evolutiongaming.kafka.journal.util.{Executors, FromFuture, ToFuture}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.consumer.Consumer
import com.evolutiongaming.skafka.producer.Producer
import com.evolutiongaming.skafka.{ClientId, CommonConfig}
import play.api.libs.json.JsValue

import scala.collection.immutable.Seq
import scala.concurrent.ExecutionContext
import scala.util.Try

trait JournalAdapter[F[_]] {

  def write(
    aws: Seq[AtomicWrite],
    metadata: Option[JsValue] = None,
    headers: Headers = Headers.Empty
  ): F[List[Try[Unit]]]

  def delete(persistenceId: PersistenceId, to: SeqNr): F[Unit]

  def lastSeqNr(persistenceId: PersistenceId, from: SeqNr): F[Option[SeqNr]]

  def replay(persistenceId: PersistenceId, range: SeqRange, max: Long)(f: PersistentRepr => Unit): F[Unit]
}

object JournalAdapter {

  def of[F[_] : Concurrent : ContextShift : FromFuture : ToFuture : Par : Timer : LogOf : Runtime : RandomId](
    toKey: ToKey,
    origin: Option[Origin],
    serializer: EventSerializer[cats.Id],
    config: KafkaJournalConfig,
    metrics: Metrics[F],
    log: Log[F],
    batching: Batching[F]
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
        KafkaProducer.Metrics(metrics(clientId))
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
          metrics = metrics.journal)
      } yield {
        journal.withLogError(log)
      }
    }

    for {
      blocking         <- Executors.blocking[F]
      kafkaProducerOf1  = kafkaProducerOf(blocking)
      kafkaConsumerOf1  = kafkaConsumerOf(blocking)
      headCacheOf1      = headCacheOf(kafkaConsumerOf1)
      eventualJournal  <- EventualCassandra.of[F](config.cassandra, metrics.eventual)
      journal          <- journal(eventualJournal)(kafkaConsumerOf1, kafkaProducerOf1, headCacheOf1)
    } yield {
      JournalAdapter[F](journal, toKey, serializer).withBatching(batching)
    }
  }

  def apply[F[_] : Monad : Clock](
    journal: Journal[F],
    toKey: ToKey,
    serializer: EventSerializer[cats.Id]
  ): JournalAdapter[F] = new JournalAdapter[F] {

    def write(aws: Seq[AtomicWrite], metadata: Option[JsValue], headers: Headers) = {
      val prs = aws.flatMap(_.payload)
      Nel.opt(prs).fold {
        List.empty[Try[Unit]].pure[F]
      } { prs =>
        val persistenceId = prs.head.persistenceId
        val key = toKey(persistenceId)
        val events = prs.map(serializer.toEvent)
        for {
          _ <- journal.append(key, events, metadata, headers)
        } yield List.empty[Try[Unit]]
      }
    }

    def delete(persistenceId: PersistenceId, to: SeqNr) = {
      val key  = toKey(persistenceId)
      journal.delete(key, to).void
    }

    def replay(persistenceId: PersistenceId, range: SeqRange, max: Long)
      (callback: PersistentRepr => Unit) = {

      val key = toKey(persistenceId)

      val stream = journal
        .read(key, range.from)
        .foldMapCmd(max) { (n, event) =>
          val seqNr = event.seqNr
          if (n > 0 && seqNr <= range.to) {
            val persistentRepr = serializer.toPersistentRepr(persistenceId, event)
            callback(persistentRepr)
            (n - 1, Stream.Cmd.take(event))
          } else {
            (n, Stream.Cmd.stop)
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

    def mapK[G[_]](f: F ~> G): JournalAdapter[G] = new JournalAdapter[G] {

      def write(aws: Seq[AtomicWrite], metadata: Option[JsValue], headers: Headers) = {
        f(self.write(aws, metadata, headers))
      }

      def delete(persistenceId: PersistenceId, to: SeqNr) = f(self.delete(persistenceId, to))

      def lastSeqNr(persistenceId: PersistenceId, from: SeqNr) = f(self.lastSeqNr(persistenceId, from))

      def replay(persistenceId: PersistenceId, range: SeqRange, max: Long)(f1: PersistentRepr => Unit) = {
        f(self.replay(persistenceId, range, max)(f1))
      }
    }


    def withBatching(batching: Batching[F])(implicit F : Monad[F]): JournalAdapter[F] = new JournalAdapter[F] {

      def write(aws: Seq[AtomicWrite], metadata: Option[JsValue], headers: Headers) = {
        if (aws.size <= 1) self.write(aws, metadata, headers)
        else for {
          batches <- batching(aws.toList)
          results <- batches.foldLeftM(List.empty[List[Try[Unit]]]) { (results, group) =>
            for {
              result <- self.write(group, metadata, headers)
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

      def replay(persistenceId: PersistenceId, range: SeqRange, max: Long)(f: PersistentRepr => Unit) = {
        self.replay(persistenceId, range, max)(f)
      }
    }
  }


  final case class Metrics[F[_]](
    journal: Option[Journal.Metrics[F]] = None,
    eventual: Option[EventualJournal.Metrics[F]] = None,
    headCache: Option[HeadCache.Metrics[F]] = None,
    producer: Option[ClientId => Producer.Metrics] = None,
    consumer: Option[ClientId => Consumer.Metrics] = None)

  object Metrics {
    def empty[F[_]]: Metrics[F] = Metrics[F]()
  }
}