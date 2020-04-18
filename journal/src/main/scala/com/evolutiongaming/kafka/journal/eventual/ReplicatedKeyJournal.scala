package com.evolutiongaming.kafka.journal.eventual

import java.time.Instant

import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import cats.{Applicative, FlatMap, ~>}
import com.evolutiongaming.catshelper.{ApplicativeThrowable, Log}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.skafka.{Offset, Topic}
import com.evolutiongaming.smetrics._


trait ReplicatedKeyJournal[F[_]] {

  def append(
    partitionOffset: PartitionOffset,
    timestamp: Instant,
    expireAfter: Option[ExpireAfter],
    events: Nel[EventRecord]
  ): F[Boolean]

  def delete(
    partitionOffset: PartitionOffset,
    timestamp: Instant,
    deleteTo: DeleteTo,
    origin: Option[Origin]
  ): F[Boolean]

  def purge(
    offset: Offset,
    timestamp: Instant
  ): F[Boolean]
}

object ReplicatedKeyJournal {

  def empty[F[_] : Applicative]: ReplicatedKeyJournal[F] = new ReplicatedKeyJournal[F] {

    def append(
      partitionOffset: PartitionOffset,
      timestamp: Instant,
      expireAfter: Option[ExpireAfter],
      events: Nel[EventRecord]
    ) = false.pure[F]

    def delete(
      partitionOffset: PartitionOffset,
      timestamp: Instant,
      deleteTo: DeleteTo,
      origin: Option[Origin]
    ) = false.pure[F]

    def purge(
      offset: Offset,
      timestamp: Instant
    ) = false.pure[F]
  }


  def apply[F[_]](key: Key, replicatedJournal: ReplicatedJournalFlat[F]): ReplicatedKeyJournal[F] = {

    new ReplicatedKeyJournal[F] {

      def append(
        partitionOffset: PartitionOffset,
        timestamp: Instant,
        expireAfter: Option[ExpireAfter],
        events: Nel[EventRecord]
      ) = {
        replicatedJournal.append(key, partitionOffset, timestamp, expireAfter, events)
      }

      def delete(
        partitionOffset: PartitionOffset,
        timestamp: Instant,
        deleteTo: DeleteTo,
        origin: Option[Origin]
      ) = {
        replicatedJournal.delete(key, partitionOffset, timestamp, deleteTo, origin)
      }

      def purge(
        offset: Offset,
        timestamp: Instant
      ) = {
        replicatedJournal.purge(key, offset, timestamp)
      }
    }
  }


  implicit class ReplicatedKeyJournalOps[F[_]](val self: ReplicatedKeyJournal[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): ReplicatedKeyJournal[G] = new ReplicatedKeyJournal[G] {

      def append(
        partitionOffset: PartitionOffset,
        timestamp: Instant,
        expireAfter: Option[ExpireAfter],
        events: Nel[EventRecord]
      ) = {
        f(self.append(partitionOffset, timestamp, expireAfter, events))
      }

      def delete(
        partitionOffset: PartitionOffset,
        timestamp: Instant,
        deleteTo: DeleteTo,
        origin: Option[Origin]
      ) = {
        f(self.delete(partitionOffset, timestamp, deleteTo, origin))
      }

      def purge(
        offset: Offset,
        timestamp: Instant
      ) = {
        f(self.purge(offset, timestamp))
      }
    }


    def withLog(
      key: Key,
      log: Log[F])(implicit
      F: FlatMap[F],
      measureDuration: MeasureDuration[F]
    ): ReplicatedKeyJournal[F] = {

      new ReplicatedKeyJournal[F] {

        def append(
          partitionOffset: PartitionOffset,
          timestamp: Instant,
          expireAfter: Option[ExpireAfter],
          events: Nel[EventRecord]
        ) = {
          for {
            d <- MeasureDuration[F].start
            r <- self.append(partitionOffset, timestamp, expireAfter, events)
            d <- d
            _ <- log.debug {
              val origin = events.head.origin
              val originStr = origin.foldMap { origin => s", origin: $origin" }
              val expireAfterStr = expireAfter.foldMap { expireAfter => s", expireAfter: $expireAfter" }
              s"$key append in ${ d.toMillis }ms, " +
                s"offset: $partitionOffset$originStr$expireAfterStr, " +
                s"events: ${ events.toList.mkString(",") }"
            }
          } yield r
        }

        def delete(
          partitionOffset: PartitionOffset,
          timestamp: Instant,
          deleteTo: DeleteTo,
          origin: Option[Origin]
        ) = {
          for {
            d <- MeasureDuration[F].start
            r <- self.delete(partitionOffset, timestamp, deleteTo, origin)
            d <- d
            _ <- log.debug {
              val originStr = origin.foldMap { origin => s", origin: $origin" }
              s"$key delete in ${ d.toMillis }ms, offset: $partitionOffset, deleteTo: $deleteTo$originStr"
            }
          } yield r
        }

        def purge(
          offset: Offset,
          timestamp: Instant
        ) = {
          for {
            d <- MeasureDuration[F].start
            r <- self.purge(offset, timestamp)
            d <- d
            _ <- log.debug(s"$key purge in ${ d.toMillis }ms, offset: $offset")
          } yield r
        }
      }
    }


    def withMetrics(
      topic: Topic,
      metrics: ReplicatedJournal.Metrics[F])(implicit
      F: FlatMap[F],
      measureDuration: MeasureDuration[F]
    ): ReplicatedKeyJournal[F] = {
      new ReplicatedKeyJournal[F] {

        def append(
          partitionOffset: PartitionOffset,
          timestamp: Instant,
          expireAfter: Option[ExpireAfter],
          events: Nel[EventRecord]
        ) = {
          for {
            d <- MeasureDuration[F].start
            r <- self.append(partitionOffset, timestamp, expireAfter, events)
            d <- d
            _ <- metrics.append(topic = topic, latency = d, events = events.size)
          } yield r
        }

        def delete(
          partitionOffset: PartitionOffset,
          timestamp: Instant,
          deleteTo: DeleteTo,
          origin: Option[Origin]
        ) = {
          for {
            d <- MeasureDuration[F].start
            r <- self.delete(partitionOffset, timestamp, deleteTo, origin)
            d <- d
            _ <- metrics.delete(topic, d)
          } yield r
        }

        def purge(
          offset: Offset,
          timestamp: Instant
        ) = {
          for {
            d <- MeasureDuration[F].start
            r <- self.purge(offset, timestamp)
            d <- d
            _ <- metrics.purge(topic, d)
          } yield r
        }
      }
    }


    def enhanceError(key: Key)(implicit F: ApplicativeThrowable[F]): ReplicatedKeyJournal[F] = {

      def error[A](msg: String, cause: Throwable) = {
        JournalError(s"ReplicatedKeyJournal.$msg failed with $cause", cause).raiseError[F, A]
      }

      new ReplicatedKeyJournal[F] {

        def append(
          partitionOffset: PartitionOffset,
          timestamp: Instant,
          expireAfter: Option[ExpireAfter],
          events: Nel[EventRecord]
        ) = {
          self
            .append(partitionOffset, timestamp, expireAfter, events)
            .handleErrorWith { a =>
              error(s"append " +
                s"key: $key, " +
                s"offset: $partitionOffset, " +
                s"timestamp: $timestamp, " +
                s"expireAfter: $expireAfter, " +
                s"events: $events", a)
            }
        }

        def delete(
          partitionOffset: PartitionOffset,
          timestamp: Instant,
          deleteTo: DeleteTo,
          origin: Option[Origin]
        ) = {
          self
            .delete(partitionOffset, timestamp, deleteTo, origin)
            .handleErrorWith { a =>
              error(
                s"delete " +
                  s"key: $key, " +
                  s"offset: $partitionOffset, " +
                  s"timestamp: $timestamp, " +
                  s"deleteTo: $deleteTo, " +
                  s"origin: $origin", a)
            }
        }

        def purge(
          offset: Offset,
          timestamp: Instant
        ) = {
          self
            .purge(offset, timestamp)
            .handleErrorWith { a =>
              error(s"purge " +
                s"key: $key, " +
                s"offset: $offset, " +
                s"timestamp: $timestamp", a)
            }
        }
      }
    }
  }
}