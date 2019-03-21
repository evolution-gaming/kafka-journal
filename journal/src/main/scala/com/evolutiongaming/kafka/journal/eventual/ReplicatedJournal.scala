package com.evolutiongaming.kafka.journal.eventual

import java.time.Instant

import cats.effect.Clock
import cats.implicits._
import cats.{Applicative, FlatMap, ~>}
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.Topic


trait ReplicatedJournal[F[_]] {

  def topics: F[Iterable[Topic]]

  def pointers(topic: Topic): F[TopicPointers]

  def append(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, events: Nel[EventRecord]): F[Unit]

  def delete(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, deleteTo: SeqNr, origin: Option[Origin]): F[Unit]

  def save(topic: Topic, pointers: TopicPointers, timestamp: Instant): F[Unit]
}

object ReplicatedJournal {

  def apply[F[_]](implicit F: ReplicatedJournal[F]): ReplicatedJournal[F] = F

  def apply[F[_] : FlatMap : Clock](journal: ReplicatedJournal[F], log: Log[F], metrics: Option[Metrics[F]]): ReplicatedJournal[F] = {
    val logging = apply[F](journal, log)
    metrics.fold(logging) { metrics => ReplicatedJournal[F](logging, metrics) }
  }

  def apply[F[_] : FlatMap : Clock](journal: ReplicatedJournal[F], log: Log[F]): ReplicatedJournal[F] = {

    implicit val log1 = log

    new ReplicatedJournal[F] {

      def topics = {
        for {
          rl     <- Latency { journal.topics }
          (r, l)  = rl
          _      <- Log[F].debug(s"topics in ${ l }ms, r: ${ r.mkString(",") }")
        } yield r
      }

      def pointers(topic: Topic) = {
        for {
          rl     <- Latency { journal.pointers(topic) }
          (r, l)  = rl
          _      <- Log[F].debug(s"$topic pointers in ${ l }ms, result: $r")
        } yield r
      }

      def append(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, events: Nel[EventRecord]) = {
        for {
          rl     <- Latency { journal.append(key, partitionOffset, timestamp, events) }
          (r, l)  = rl
          _      <- Log[F].debug {
            val origin = events.head.origin
            val originStr = origin.fold("") { origin => s", origin: $origin" }
            s"$key append in ${ l }ms, offset: $partitionOffset, events: ${ events.mkString(",") }$originStr"
          }
        } yield r
      }

      def delete(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, deleteTo: SeqNr, origin: Option[Origin]) = {
        for {
          rl     <- Latency { journal.delete(key, partitionOffset, timestamp, deleteTo, origin) }
          (r, l)  = rl
          _      <- Log[F].debug {
            val originStr = origin.fold("") { origin => s", origin: $origin" }
            s"$key delete in ${ l }ms, offset: $partitionOffset, deleteTo: $deleteTo$originStr"
          }
        } yield r
      }

      def save(topic: Topic, pointers: TopicPointers, timestamp: Instant) = {
        for {
          rl     <- Latency { journal.save(topic, pointers, timestamp) }
          (r, l)  = rl
          _      <- Log[F].debug(s"$topic save in ${ l }ms, pointers: $pointers, timestamp: $timestamp")
        } yield r
      }
    }
  }


  def apply[F[_] : FlatMap : Clock](journal: ReplicatedJournal[F], metrics: Metrics[F]): ReplicatedJournal[F] = {

    new ReplicatedJournal[F] {

      def topics = {
        for {
          rl     <- Latency { journal.topics }
          (r, l)  = rl
          _      <- metrics.topics(l)
        } yield r
      }

      def pointers(topic: Topic) = {
        for {
          rl     <- Latency { journal.pointers(topic) }
          (r, l)  = rl
          _      <- metrics.pointers(l)
        } yield r
      }

      def append(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, events: Nel[EventRecord]) = {
        for {
          rl     <- Latency { journal.append(key, partitionOffset, timestamp, events) }
          (r, l)  = rl
          _      <- metrics.append(topic = key.topic, latency = l, events = events.size)
        } yield r
      }

      def delete(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, deleteTo: SeqNr, origin: Option[Origin]) = {
        for {
          rl     <- Latency { journal.delete(key, partitionOffset, timestamp, deleteTo, origin) }
          (r, l)  = rl
          _      <- metrics.delete(key.topic, l)
        } yield r
      }

      def save(topic: Topic, pointers: TopicPointers, timestamp: Instant) = {
        for {
          rl     <- Latency { journal.save(topic, pointers, timestamp) }
          (r, l)  = rl
          _      <- metrics.save(topic, l)
        } yield r
      }
    }
  }


  def empty[F[_] : Applicative]: ReplicatedJournal[F] = new ReplicatedJournal[F] {

    def topics = Iterable.empty[Topic].pure[F]

    def pointers(topic: Topic) = TopicPointers.Empty.pure[F]

    def append(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, events: Nel[EventRecord]) = ().pure[F]

    def delete(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, deleteTo: SeqNr, origin: Option[Origin]) = ().pure[F]

    def save(topic: Topic, pointers: TopicPointers, timestamp: Instant) = ().pure[F]
  }


  trait Metrics[F[_]] {

    def topics(latency: Long): F[Unit]

    def pointers(latency: Long): F[Unit]

    def append(topic: Topic, latency: Long, events: Int): F[Unit]

    def delete(topic: Topic, latency: Long): F[Unit]

    def save(topic: Topic, latency: Long): F[Unit]
  }

  object Metrics {

    def empty[F[_]](unit: F[Unit]): Metrics[F] = new Metrics[F] {

      def topics(latency: Long) = unit

      def pointers(latency: Long) = unit

      def append(topic: Topic, latency: Long, events: Int) = unit

      def delete(topic: Topic, latency: Long) = unit

      def save(topic: Topic, latency: Long) = unit
    }

    def empty[F[_] : Applicative]: Metrics[F] = empty(().pure[F])
  }


  implicit class ReplicatedJournalOps[F[_]](val self: ReplicatedJournal[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): ReplicatedJournal[G] = new ReplicatedJournal[G] {

      def topics = f(self.topics)

      def pointers(topic: Topic) = f(self.pointers(topic))

      def append(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, events: Nel[EventRecord]) = {
        f(self.append(key, partitionOffset, timestamp, events))
      }

      def delete(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, deleteTo: SeqNr, origin: Option[Origin]) = {
        f(self.delete(key, partitionOffset, timestamp, deleteTo, origin))
      }

      def save(topic: Topic, pointers: TopicPointers, timestamp: Instant) = {
        f(self.save(topic, pointers, timestamp))
      }
    }


    def withLog(log: Log[F])(implicit flatMap: FlatMap[F], clock: Clock[F]): ReplicatedJournal[F] = {
      ReplicatedJournal[F](self, log)
    }


    def withMetrics(metrics: Metrics[F])(implicit flatMap: FlatMap[F], clock: Clock[F]): ReplicatedJournal[F] = {
      ReplicatedJournal(self, metrics)
    }
  }
}