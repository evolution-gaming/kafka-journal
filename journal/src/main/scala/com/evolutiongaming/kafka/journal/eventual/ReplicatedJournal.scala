package com.evolutiongaming.kafka.journal.eventual

import java.time.Instant

import com.evolutiongaming.kafka.journal.FlatMap._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.Topic

import scala.compat.Platform


trait ReplicatedJournal[F[_]] {
  // TODO not used
  def topics(): F[Iterable[Topic]]

  def pointers(topic: Topic): F[TopicPointers]

  def append(key: Key, timestamp: Instant, events: Nel[ReplicatedEvent], deleteTo: Option[SeqNr]): F[Unit]

  def delete(key: Key, timestamp: Instant, deleteTo: SeqNr, bound: Boolean): F[Unit]

  def save(topic: Topic, pointers: TopicPointers, timestamp: Instant): F[Unit]
}

object ReplicatedJournal {

  def apply[F[_]](implicit F: ReplicatedJournal[F]): ReplicatedJournal[F] = F

  def apply[F[_] : FlatMap](journal: ReplicatedJournal[F], metrics: Metrics[F]): ReplicatedJournal[F] = {

    new ReplicatedJournal[F] {

      def latency[T](func: => F[T]): F[(T, Long)] = {
        val start = Platform.currentTime
        for {
          result <- func
          latency = Platform.currentTime - start
        } yield (result, latency)
      }

      def topics() = {
        for {
          tuple <- latency { journal.topics() }
          (result, latency) = tuple
          _ <- metrics.topics(latency)
        } yield result
      }

      def pointers(topic: Topic) = {
        for {
          tuple <- latency { journal.pointers(topic) }
          (result, latency) = tuple
          _ <- metrics.pointers(latency)
        } yield result
      }

      def append(key: Key, timestamp: Instant, events: Nel[ReplicatedEvent], deleteTo: Option[SeqNr]) = {
        for {
          tuple <- latency { journal.append(key, timestamp, events, deleteTo) }
          (result, latency) = tuple
          _ <- metrics.append(topic = key.topic, latency = latency, events = events.size)
        } yield result
      }

      def delete(key: Key, timestamp: Instant, deleteTo: SeqNr, bound: Boolean) = {
        for {
          tuple <- latency { journal.delete(key, timestamp, deleteTo, bound) }
          (result, latency) = tuple
          _ <- metrics.delete(key.topic, latency, bound)
        } yield result
      }

      def save(topic: Topic, pointers: TopicPointers, timestamp: Instant) = {
        for {
          tuple <- latency { journal.save(topic, pointers, timestamp) }
          (result, latency) = tuple
          _ <- metrics.save(topic, latency)
        } yield result
      }
    }
  }

  def apply[F[_] : FlatMap](journal: ReplicatedJournal[F], log: Log[F]): ReplicatedJournal[F] = {

    def latency[T](func: => F[T]): F[(T, Long)] = {
      val start = Platform.currentTime
      for {
        result <- func
        latency = Platform.currentTime - start
      } yield (result, latency)
    }

    new ReplicatedJournal[F] {

      def topics() = {
        for {
          tuple <- latency { journal.topics() }
          (result, latency) = tuple
          _ <- log.debug(s"topics in ${ latency }ms, topics: ${ result.mkString(",") }")
        } yield result
      }

      def pointers(topic: Topic) = {
        for {
          tuple <- latency { journal.pointers(topic) }
          (result, latency) = tuple
          _ <- log.debug(s"pointers in ${ latency }ms, topic: $topic, pointers: $result")
        } yield result
      }

      def append(key: Key, timestamp: Instant, events: Nel[ReplicatedEvent], deleteTo: Option[SeqNr]) = {
        for {
          tuple <- latency { journal.append(key, timestamp, events, deleteTo) }
          (result, latency) = tuple
          _ <- log.debug(s"append in ${ latency }ms, key: $key, deleteTo: $deleteTo, events: ${ events.mkString(",") }")
        } yield result
      }

      def delete(key: Key, timestamp: Instant, deleteTo: SeqNr, bound: Boolean) = {
        for {
          tuple <- latency { journal.delete(key, timestamp, deleteTo, bound) }
          (result, latency) = tuple
          _ <- log.debug(s"delete in ${ latency }ms, key: $key, deleteTo: $deleteTo, bound: $bound")
        } yield result
      }

      def save(topic: Topic, pointers: TopicPointers, timestamp: Instant) = {
        for {
          tuple <- latency { journal.save(topic, pointers, timestamp) }
          (result, latency) = tuple
          _ <- log.debug(s"save in ${ latency }ms, topic: $topic, pointers: $pointers")
        } yield result
      }

      override def toString = journal.toString
    }
  }


  trait Metrics[F[_]] {

    def topics(latency: Long): F[Unit]

    def pointers(latency: Long): F[Unit]

    def append(topic: Topic, latency: Long, events: Int): F[Unit]

    def delete(topic: Topic, latency: Long, bound: Boolean): F[Unit]

    def save(topic: Topic, latency: Long): F[Unit]
  }

  object Metrics {

    def empty[F[_]](unit: F[Unit]): Metrics[F] = new Metrics[F] {

      def topics(latency: Long) = unit

      def pointers(latency: Long) = unit

      def append(topic: Topic, latency: Long, events: Int) = unit

      def delete(topic: Topic, latency: Long, bound: Boolean) = unit

      def save(topic: Topic, latency: Long) = unit
    }
  }
}