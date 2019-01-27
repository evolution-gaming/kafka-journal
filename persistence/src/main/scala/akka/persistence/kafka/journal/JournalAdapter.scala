package akka.persistence.kafka.journal

import akka.persistence.{AtomicWrite, PersistentRepr}
import cats.effect._
import cats.implicits._
import cats.{Monad, ~>}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandra
import com.evolutiongaming.kafka.journal.stream.Stream
import com.evolutiongaming.kafka.journal.util.ClockHelper._
import com.evolutiongaming.kafka.journal.util.{Executors, FromFuture, Par, Runtime, ToFuture}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.consumer.Consumer
import com.evolutiongaming.skafka.producer.Producer
import com.evolutiongaming.skafka.{ClientId, CommonConfig}

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

  def of[F[_] : Concurrent : ContextShift : FromFuture : ToFuture : Par : Timer : LogOf : Runtime](
    toKey: ToKey,
    origin: Option[Origin],
    serializer: EventSerializer,
    config: KafkaJournalConfig,
    metrics: Metrics[F],
    actorLog: ActorLog): Resource[F, JournalAdapter[F]] = {

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

    def journal(
      eventualJournal: EventualJournal[F])(implicit
      kafkaConsumerOf: KafkaConsumerOf[F],
      kafkaProducerOf: KafkaProducerOf[F]) = {

      Journal.of[F](
        origin = origin,
        config = config.journal,
        eventualJournal = eventualJournal,
        metrics = metrics.journal)
    }

    for {
      blocking         <- Executors.blocking[F]
      kafkaProducerOf1  = kafkaProducerOf(blocking)
      kafkaConsumerOf1  = kafkaConsumerOf(blocking)
      eventualJournal  <- EventualCassandra.of[F](config.cassandra, metrics.eventual)
      journal          <- journal(eventualJournal)(kafkaConsumerOf1, kafkaProducerOf1)
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

        for {
          _ <- Log[F].debug(s"$persistenceId replay, range: $range")
          _ <- stream.drain
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