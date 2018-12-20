package com.evolutiongaming.kafka.journal.eventual

import java.time.Instant

import cats.{Applicative, FlatMap}
import cats.implicits._
import cats.effect.Clock
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.Topic


trait ReplicatedJournal[F[_]] {

  def topics: F[Iterable[Topic]]

  def pointers(topic: Topic): F[TopicPointers]

  def append(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, events: Nel[ReplicatedEvent]): F[Unit]

  def delete(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, deleteTo: SeqNr, origin: Option[Origin]): F[Unit]

  def save(topic: Topic, pointers: TopicPointers, timestamp: Instant): F[Unit]
}

object ReplicatedJournal {

  def apply[F[_] : FlatMap : Clock : Log](journal: ReplicatedJournal[F]): ReplicatedJournal[F] = new ReplicatedJournal[F] {

    def topics = {
      for {
        tuple            <- Latency { journal.topics }
        (result, latency) = tuple
        _                <- Log[F].debug(s"topics in ${ latency }ms, result: ${ result.mkString(",") }")
      } yield result
    }

    def pointers(topic: Topic) = {
      for {
        tuple            <- Latency { journal.pointers(topic) }
        (result, latency) = tuple
        _                <- Log[F].debug(s"$topic pointers in ${ latency }ms, result: $result")
      } yield result
    }

    def append(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, events: Nel[ReplicatedEvent]) = {
      for {
        tuple            <- Latency { journal.append(key, partitionOffset, timestamp, events) }
        (result, latency) = tuple
        _                <- Log[F].debug {
          val origin = events.head.origin
          val originStr = origin.fold("") { origin => s", origin: $origin" }
          s"$key append in ${ latency }ms, offset: $partitionOffset, events: ${ events.mkString(",") }$originStr"
        }
      } yield result
    }

    def delete(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, deleteTo: SeqNr, origin: Option[Origin]) = {
      for {
        tuple            <- Latency { journal.delete(key, partitionOffset, timestamp, deleteTo, origin) }
        (result, latency) = tuple
        _                <- Log[F].debug {
          val originStr = origin.fold("") { origin => s", origin: $origin" }
          s"$key delete in ${ latency }ms, offset: $partitionOffset, deleteTo: $deleteTo$originStr"
        }
      } yield result
    }

    def save(topic: Topic, pointers: TopicPointers, timestamp: Instant) = {
      for {
        tuple            <- Latency { journal.save(topic, pointers, timestamp) }
        (result, latency) = tuple
        _                <- Log[F].debug(s"$topic save in ${ latency }ms, pointers: $pointers, timestamp: $timestamp")
      } yield result
    }
  }


  def apply[F[_] : FlatMap : Clock](journal: ReplicatedJournal[F], metrics: Metrics[F]): ReplicatedJournal[F] = new ReplicatedJournal[F] {

    def topics = {
      for {
        tuple            <- Latency { journal.topics }
        (result, latency) = tuple
        _                <- metrics.topics(latency)
      } yield result
    }

    def pointers(topic: Topic) = {
      for {
        tuple            <- Latency { journal.pointers(topic) }
        (result, latency) = tuple
        _                <- metrics.pointers(latency)
      } yield result
    }

    def append(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, events: Nel[ReplicatedEvent]) = {
      for {
        tuple            <- Latency { journal.append(key, partitionOffset, timestamp, events) }
        (result, latency) = tuple
        _                <- metrics.append(topic = key.topic, latency = latency, events = events.size)
      } yield result
    }

    def delete(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, deleteTo: SeqNr, origin: Option[Origin]) = {
      for {
        tuple            <- Latency { journal.delete(key, partitionOffset, timestamp, deleteTo, origin) }
        (result, latency) = tuple
        _                <- metrics.delete(key.topic, latency)
      } yield result
    }

    def save(topic: Topic, pointers: TopicPointers, timestamp: Instant) = {
      for {
        tuple            <- Latency { journal.save(topic, pointers, timestamp) }
        (result, latency) = tuple
        _                <- metrics.save(topic, latency)
      } yield result
    }
  }


  def empty[F[_] : Applicative]: ReplicatedJournal[F] = new ReplicatedJournal[F] {

    def topics = Iterable.empty[Topic].pure[F]

    def pointers(topic: Topic) = TopicPointers.Empty.pure[F]

    def append(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, events: Nel[ReplicatedEvent]) = ().pure[F]

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
}