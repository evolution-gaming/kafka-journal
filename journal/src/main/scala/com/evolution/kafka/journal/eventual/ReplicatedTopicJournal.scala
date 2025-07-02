package com.evolution.kafka.journal.eventual

import cats.effect.Resource
import cats.effect.syntax.all.*
import cats.syntax.all.*
import cats.{Applicative, Monad, ~>}
import com.evolution.kafka.journal.*
import com.evolutiongaming.catshelper.{BracketThrowable, Log, MeasureDuration, MonadThrowable}
import com.evolutiongaming.skafka.{Partition, Topic}

trait ReplicatedTopicJournal[F[_]] {

  def apply(partition: Partition): Resource[F, ReplicatedPartitionJournal[F]]
}

object ReplicatedTopicJournal {

  def empty[F[_]: Applicative]: ReplicatedTopicJournal[F] = {
    class Empty
    new Empty with ReplicatedTopicJournal[F] {
      def apply(partition: Partition) = {
        ReplicatedPartitionJournal
          .empty[F]
          .pure[F]
          .toResource
      }
    }
  }

  /*def apply[F[_]: Applicative](
    topic: Topic,
    replicatedJournal: ReplicatedJournalFlat[F]
  ): ReplicatedTopicJournal[F] = {

    class Main
    new Main with ReplicatedTopicJournal[F] {

      def pointer(partition: Partition): F[Option[Offset]] = replicatedJournal.pointer(topic, partition)

      def journal(id: String) = {
        val key = Key(id = id, topic = topic)
        ReplicatedKeyJournal(key, replicatedJournal)
          .pure[F]
          .toResource
      }
    }
  }*/

  private sealed abstract class WithLog

  private sealed abstract class WithMetrics

  private sealed abstract class EnhanceError

  private sealed abstract class MapK

  implicit class ReplicatedTopicJournalOps[F[_]](val self: ReplicatedTopicJournal[F]) extends AnyVal {

    def mapK[G[_]](
      f: F ~> G,
    )(implicit
      B: BracketThrowable[F],
      GT: BracketThrowable[G],
    ): ReplicatedTopicJournal[G] = {
      new MapK with ReplicatedTopicJournal[G] {

        def apply(partition: Partition) = {
          self
            .apply(partition)
            .map(_.mapK(f))
            .mapK(f)
        }
      }
    }

    def withLog(
      topic: Topic,
      log: Log[F],
    )(implicit
      F: Monad[F],
      measureDuration: MeasureDuration[F],
    ): ReplicatedTopicJournal[F] = {
      new WithLog with ReplicatedTopicJournal[F] {

        def apply(partition: Partition) = {
          self
            .apply(partition)
            .map { _.withLog(topic, partition, log) }
        }
      }
    }

    def withMetrics(
      topic: Topic,
      metrics: ReplicatedJournal.Metrics[F],
    )(implicit
      F: Monad[F],
      measureDuration: MeasureDuration[F],
    ): ReplicatedTopicJournal[F] = {
      new WithMetrics with ReplicatedTopicJournal[F] {

        def apply(partition: Partition) = {
          self
            .apply(partition)
            .map { _.withMetrics(topic, metrics) }
        }
      }
    }

    def enhanceError(
      topic: Topic,
    )(implicit
      F: MonadThrowable[F],
    ): ReplicatedTopicJournal[F] = {

      def journalError(msg: String, cause: Throwable) = {
        JournalError(s"ReplicatedTopicJournal.$msg failed with $cause", cause)
      }

      new EnhanceError with ReplicatedTopicJournal[F] {

        def apply(partition: Partition) = {
          self
            .apply(partition)
            .map { _.enhanceError(topic, partition) }
            .adaptError { case a => journalError(s"journal topic: $topic, partition: $partition", a) }
        }
      }
    }
  }
}
