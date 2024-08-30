package com.evolutiongaming.kafka.journal.eventual

import cats.data.NonEmptyList as Nel
import cats.syntax.all.*
import cats.{Applicative, FlatMap, ~>}
import com.evolutiongaming.catshelper.{ApplicativeThrowable, Log, MeasureDuration}
import com.evolutiongaming.kafka.journal.*
import com.evolutiongaming.kafka.journal.eventual.ReplicatedKeyJournal.Changed
import com.evolutiongaming.skafka.{Offset, Partition, Topic}

import java.time.Instant

/**
  * Write-only implementation of a journal stored to eventual storage, i.e. Cassandra. 
  * The interface provides actions against a _single key_ thus the name.
  * 
  * Alternative API provided by [[ReplicatedJournalFlat]] is more suitable for general use.
  */
trait ReplicatedKeyJournal[F[_]] {

  def append(
    offset: Offset,
    timestamp: Instant,
    expireAfter: Option[ExpireAfter],
    events: Nel[EventRecord[EventualPayloadAndType]],
  ): F[Changed]

  def delete(
    offset: Offset,
    timestamp: Instant,
    deleteTo: DeleteTo,
    origin: Option[Origin],
  ): F[Changed]

  def purge(
    offset: Offset,
    timestamp: Instant,
  ): F[Changed]
}

object ReplicatedKeyJournal {

  type Changed = Boolean

  def empty[F[_]: Applicative]: ReplicatedKeyJournal[F] = const(false.pure[F])

  def const[F[_]](value: F[Boolean]): ReplicatedKeyJournal[F] = {
    class Const
    new Const with ReplicatedKeyJournal[F] {

      def append(
        offset: Offset,
        timestamp: Instant,
        expireAfter: Option[ExpireAfter],
        events: Nel[EventRecord[EventualPayloadAndType]],
      ) = value

      def delete(
        offset: Offset,
        timestamp: Instant,
        deleteTo: DeleteTo,
        origin: Option[Origin],
      ) = value

      def purge(
        offset: Offset,
        timestamp: Instant,
      ) = value
    }
  }

  private abstract sealed class WithLog

  private abstract sealed class WithMetrics

  private abstract sealed class MapK

  implicit class ReplicatedKeyJournalOps[F[_]](val self: ReplicatedKeyJournal[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): ReplicatedKeyJournal[G] = {
      new MapK with ReplicatedKeyJournal[G] {

        def append(
          offset: Offset,
          timestamp: Instant,
          expireAfter: Option[ExpireAfter],
          events: Nel[EventRecord[EventualPayloadAndType]],
        ) = {
          f(self.append(offset, timestamp, expireAfter, events))
        }

        def delete(
          offset: Offset,
          timestamp: Instant,
          deleteTo: DeleteTo,
          origin: Option[Origin],
        ) = {
          f(self.delete(offset, timestamp, deleteTo, origin))
        }

        def purge(
          offset: Offset,
          timestamp: Instant,
        ) = {
          f(self.purge(offset, timestamp))
        }
      }
    }

    def withLog(key: Key, partition: Partition, log: Log[F])(
      implicit F: FlatMap[F],
      measureDuration: MeasureDuration[F],
    ): ReplicatedKeyJournal[F] = {

      new WithLog with ReplicatedKeyJournal[F] {

        def append(
          offset: Offset,
          timestamp: Instant,
          expireAfter: Option[ExpireAfter],
          events: Nel[EventRecord[EventualPayloadAndType]],
        ) = {
          for {
            d <- MeasureDuration[F].start
            r <- self.append(offset, timestamp, expireAfter, events)
            d <- d
            _ <- log.debug {
              val origin         = events.head.origin
              val originStr      = origin.foldMap { origin => s", origin: $origin" }
              val expireAfterStr = expireAfter.foldMap { expireAfter => s", expireAfter: $expireAfter" }
              s"$key append in ${d.toMillis}ms, " +
                s"offset: $partition:$offset$originStr$expireAfterStr, " +
                s"events: ${events.toList.mkString(",")}"
            }
          } yield r
        }

        def delete(
          offset: Offset,
          timestamp: Instant,
          deleteTo: DeleteTo,
          origin: Option[Origin],
        ) = {
          for {
            d <- MeasureDuration[F].start
            r <- self.delete(offset, timestamp, deleteTo, origin)
            d <- d
            _ <- log.debug {
              val originStr = origin.foldMap { origin => s", origin: $origin" }
              s"$key delete in ${d.toMillis}ms, offset: $partition:$offset, deleteTo: $deleteTo$originStr"
            }
          } yield r
        }

        def purge(
          offset: Offset,
          timestamp: Instant,
        ) = {
          for {
            d <- MeasureDuration[F].start
            r <- self.purge(offset, timestamp)
            d <- d
            _ <- log.debug(s"$key purge in ${d.toMillis}ms, offset: $partition:$offset")
          } yield r
        }
      }
    }

    def withMetrics(
      topic: Topic,
      metrics: ReplicatedJournal.Metrics[F],
    )(implicit F: FlatMap[F], measureDuration: MeasureDuration[F]): ReplicatedKeyJournal[F] = {
      new WithMetrics with ReplicatedKeyJournal[F] {

        def append(
          offset: Offset,
          timestamp: Instant,
          expireAfter: Option[ExpireAfter],
          events: Nel[EventRecord[EventualPayloadAndType]],
        ) = {
          for {
            d <- MeasureDuration[F].start
            r <- self.append(offset, timestamp, expireAfter, events)
            d <- d
            _ <- metrics.append(topic = topic, latency = d, events = events.size)
          } yield r
        }

        def delete(
          offset: Offset,
          timestamp: Instant,
          deleteTo: DeleteTo,
          origin: Option[Origin],
        ) = {
          for {
            d <- MeasureDuration[F].start
            r <- self.delete(offset, timestamp, deleteTo, origin)
            d <- d
            _ <- metrics.delete(topic, d)
          } yield r
        }

        def purge(
          offset: Offset,
          timestamp: Instant,
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

    def enhanceError(key: Key, partition: Partition)(implicit F: ApplicativeThrowable[F]): ReplicatedKeyJournal[F] = {

      def error[A](msg: String, cause: Throwable) = {
        JournalError(s"ReplicatedKeyJournal.$msg failed with $cause", cause).raiseError[F, A]
      }

      new ReplicatedKeyJournal[F] {

        def append(
          offset: Offset,
          timestamp: Instant,
          expireAfter: Option[ExpireAfter],
          events: Nel[EventRecord[EventualPayloadAndType]],
        ) = {
          self
            .append(offset, timestamp, expireAfter, events)
            .handleErrorWith { a =>
              error(
                s"append " +
                  s"key: $key, " +
                  s"offset: $partition:$offset, " +
                  s"timestamp: $timestamp, " +
                  s"expireAfter: $expireAfter, " +
                  s"events: $events",
                a,
              )
            }
        }

        def delete(
          offset: Offset,
          timestamp: Instant,
          deleteTo: DeleteTo,
          origin: Option[Origin],
        ) = {
          self
            .delete(offset, timestamp, deleteTo, origin)
            .handleErrorWith { a =>
              error(
                s"delete " +
                  s"key: $key, " +
                  s"offset: $partition:$offset, " +
                  s"timestamp: $timestamp, " +
                  s"deleteTo: $deleteTo, " +
                  s"origin: $origin",
                a,
              )
            }
        }

        def purge(
          offset: Offset,
          timestamp: Instant,
        ) = {
          self
            .purge(offset, timestamp)
            .handleErrorWith { a =>
              error(
                s"purge " +
                  s"key: $key, " +
                  s"offset: $offset, " +
                  s"timestamp: $timestamp",
                a,
              )
            }
        }
      }
    }
  }
}
