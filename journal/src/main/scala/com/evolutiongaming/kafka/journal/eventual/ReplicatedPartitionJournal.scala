package com.evolutiongaming.kafka.journal.eventual

import cats.effect.Resource
import cats.syntax.all._
import cats.{Applicative, Defer, Monad, ~>}
import com.evolutiongaming.catshelper.{BracketThrowable, Log, MeasureDuration, MonadThrowable}
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.skafka.{Offset, Partition, Topic}

import java.time.Instant

trait ReplicatedPartitionJournal[F[_]] {

  def offsets: ReplicatedPartitionJournal.Offsets[F]

  def journal(id: String): Resource[F, ReplicatedKeyJournal[F]]
}

object ReplicatedPartitionJournal {


  def empty[F[_]: Applicative]: ReplicatedPartitionJournal[F] = {
    class Empty
    new Empty with ReplicatedPartitionJournal[F] {

      def offsets = {
        new Empty with Offsets[F] {

          def get = none[Offset].pure[F]

          def create(offset: Offset, timestamp: Instant) = ().pure[F]

          def update(offset: Offset, timestamp: Instant) = ().pure[F]
        }
      }

      def journal(id: String) = {
        ReplicatedKeyJournal
          .empty[F]
          .pure[F]
          .toResource
      }
    }
  }

  trait Offsets[F[_]] {
    def get: F[Option[Offset]]

    def create(offset: Offset, timestamp: Instant): F[Unit]

    def update(offset: Offset, timestamp: Instant): F[Unit]
  }


  private sealed abstract class WithLog

  private sealed abstract class WithMetrics

  private sealed abstract class EnhanceError

  private sealed abstract class MapK

  implicit class ReplicatedPartitionJournalOps[F[_]](val self: ReplicatedPartitionJournal[F]) extends AnyVal {

    def mapK[G[_]](
      f: F ~> G)(implicit
      B: BracketThrowable[F],
      D: Defer[G],
      G: Applicative[G]
    ): ReplicatedPartitionJournal[G] = {
      new MapK with ReplicatedPartitionJournal[G] {

        def offsets = {
          new MapK with Offsets[G] {

            def get = f(self.offsets.get)

            def create(offset: Offset, timestamp: Instant) = f(self.offsets.create(offset, timestamp))

            def update(offset: Offset, timestamp: Instant) = f(self.offsets.update(offset, timestamp))
          }
        }

        def journal(id: String) = {
          self
            .journal(id)
            .map(_.mapK(f))
            .mapK(f)
        }
      }
    }


    def withLog(
      topic: Topic,
      partition: Partition,
      log: Log[F])(implicit
      F: Monad[F],
      measureDuration: MeasureDuration[F]
    ): ReplicatedPartitionJournal[F] = {
      new WithLog with ReplicatedPartitionJournal[F] {

        def offsets = {
          new WithLog with Offsets[F] {

            def get = {
              for {
                d <- MeasureDuration[F].start
                r <- self.offsets.get
                d <- d
                _ <- log.debug(s"$topic offsets.get in ${ d.toMillis }ms, partition: $partition, result: $r")
              } yield r
            }

            def create(offset: Offset, timestamp: Instant) = {
              for {
                d <- MeasureDuration[F].start
                r <- self.offsets.create(offset, timestamp)
                d <- d
                _ <- log.debug(s"$topic offsets.create in ${ d.toMillis }ms, partition: $partition, offset: $offset, timestamp: $timestamp")
              } yield r
            }

            def update(offset: Offset, timestamp: Instant) = {
              for {
                d <- MeasureDuration[F].start
                r <- self.offsets.update(offset, timestamp)
                d <- d
                _ <- log.debug(s"$topic offsets.update in ${ d.toMillis }ms, partition: $partition, offset: $offset, timestamp: $timestamp")
              } yield r
            }
          }
        }

        def journal(id: String) = {
          self
            .journal(id)
            .map { _.withLog(Key(id = id, topic = topic), partition, log) }
        }
      }
    }


    def withMetrics(
      topic: Topic,
      metrics: ReplicatedJournal.Metrics[F])(implicit
      F: Monad[F],
      measureDuration: MeasureDuration[F]
    ): ReplicatedPartitionJournal[F] = {
      new WithMetrics with ReplicatedPartitionJournal[F] {


        def offsets = {
          new WithMetrics with Offsets[F] {

            def get: F[Option[Offset]] = {
              for {
                d <- MeasureDuration[F].start
                r <- self.offsets.get
                d <- d
                _ <- metrics.offsetsGet(d)
              } yield r
            }

            def create(offset: Offset, timestamp: Instant) = {
              for {
                d <- MeasureDuration[F].start
                r <- self.offsets.create(offset, timestamp)
                d <- d
                _ <- metrics.offsetsCreate(topic, d)
              } yield r
            }

            def update(offset: Offset, timestamp: Instant) = {
              for {
                d <- MeasureDuration[F].start
                r <- self.offsets.update(offset, timestamp)
                d <- d
                _ <- metrics.offsetsUpdate(topic, d)
              } yield r
            }
          }
        }

        def journal(id: String) = {
          self
            .journal(id)
            .map { _.withMetrics(topic, metrics) }
        }
      }
    }

    def enhanceError(
      topic: Topic,
      partition: Partition)(implicit
      F: MonadThrowable[F]
    ): ReplicatedPartitionJournal[F] = {

      def journalError(msg: String, cause: Throwable) = {
        JournalError(s"ReplicatedPartitionJournal.$msg failed with $cause", cause)
      }

      new EnhanceError with ReplicatedPartitionJournal[F] {


        def offsets = {
          new EnhanceError with Offsets[F] {

            def get: F[Option[Offset]] = {
              self
                .offsets
                .get
                .adaptError { case a => journalError(s"offsets.get topic: $topic, partition: $partition", a) }
            }

            def create(offset: Offset, timestamp: Instant) = {
              self
                .offsets
                .create(offset, timestamp)
                .adaptError { case a =>
                  journalError(s"offsets.create " +
                    s"topic: $topic, " +
                    s"partition: $partition, " +
                    s"offset: $offset, " +
                    s"timestamp: $timestamp", a)
                }
            }

            def update(offset: Offset, timestamp: Instant) = {
              self
                .offsets
                .update(offset, timestamp)
                .adaptError { case a =>
                  journalError(s"offsets.update " +
                    s"topic: $topic, " +
                    s"partition: $partition, " +
                    s"offset: $offset, " +
                    s"timestamp: $timestamp", a)
                }
            }
          }
        }

        def journal(id: String) = {
          val key = Key(id = id, topic = topic)
          self
            .journal(id)
            .map { _.enhanceError(key, partition) }
            .adaptError { case a => journalError(s"journal key: $key", a) }
        }
      }
    }
  }
}