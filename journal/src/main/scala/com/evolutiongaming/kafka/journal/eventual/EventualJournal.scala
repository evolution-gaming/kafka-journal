package com.evolutiongaming.kafka.journal.eventual

import cats._
import cats.effect.Clock
import cats.implicits._
import com.evolutiongaming.kafka.journal.FoldWhile._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.skafka.Topic

trait EventualJournal[F[_]] {

  def pointers(topic: Topic): F[TopicPointers]

  def read[S](key: Key, from: SeqNr, s: S)(f: Fold[S, ReplicatedEvent]): F[Switch[S]]

  // TODO not Use Pointer until tested
  def pointer(key: Key): F[Option[Pointer]]
}

object EventualJournal {

//  def apply[F[_]](implicit F: EventualJournal[F]): EventualJournal[F] = F

  def apply[F[_] : FlatMap : Log : Clock](journal: EventualJournal[F]): EventualJournal[F] = new EventualJournal[F] {

    def pointers(topic: Topic) = {
      for {
        tuple            <- Latency { journal.pointers(topic) }
        (result, latency) = tuple
        _                 = Log[F].debug(s"$topic pointers in ${ latency }ms, result: $result")
      } yield result
    }

    def read[S](key: Key, from: SeqNr, s: S)(f: Fold[S, ReplicatedEvent]) = {
      for {
        tuple            <- Latency { journal.read(key, from, s)(f) }
        (result, latency) = tuple
        _                 = Log[F].debug(s"$key read in ${ latency }ms, from: $from, state: $s, result: $result")
      } yield result
    }

    def pointer(key: Key) = {
      for {
        tuple            <- Latency { journal.pointer(key) }
        (result, latency) = tuple
        _                 = Log[F].debug(s"$key pointer in ${ latency }ms, result: $result")
      } yield result
    }
  }


  def apply[F[_] : FlatMap : Log : Clock](
    journal: EventualJournal[F],
    metrics: Metrics[F]): EventualJournal[F] = new EventualJournal[F] {

    def pointers(topic: Topic) = {
      for {
        tuple            <- Latency { journal.pointers(topic) }
        (result, latency) = tuple
        _                <- metrics.pointers(topic, latency)
      } yield result
    }

    def read[S](key: Key, from: SeqNr, s: S)(f: Fold[S, ReplicatedEvent]) = {
      val ff: Fold[(S, Int), ReplicatedEvent] = {
        case ((s, n), e) => f(s, e).map { s => (s, n + 1) }
      }
      for {
        tuple            <- Latency { journal.read(key, from, (s, 0))(ff) }
        (switch, latency) = tuple
        (_, events)       = switch.s
        _                <- metrics.read(topic = key.topic, latency = latency, events = events)
        result            = switch.map { case (s, _) => s }
      } yield result
    }

    def pointer(key: Key) = {
      for {
        tuple            <- Latency { journal.pointer(key) }
        (result, latency) = tuple
        _                <- metrics.pointer(key.topic, latency)
      } yield result
    }
  }


  def empty[F[_] : Applicative]: EventualJournal[F] = new EventualJournal[F] {

    def pointers(topic: Topic) = TopicPointers.Empty.pure[F]

    def read[S](key: Key, from: SeqNr, state: S)(f: Fold[S, ReplicatedEvent]) = state.continue.pure[F]

    def pointer(key: Key) = none[Pointer].pure[F]
  }


  trait Metrics[F[_]] {

    def pointers(topic: Topic, latency: Long): F[Unit]

    def read(topic: Topic, latency: Long, events: Int): F[Unit]

    def pointer(topic: Topic, latency: Long): F[Unit]
  }

  object Metrics {

    def empty[F[_]](unit: F[Unit]): Metrics[F] = new Metrics[F] {

      def pointers(topic: Topic, latency: Long) = unit

      def read(topic: Topic, latency: Long, events: Int) = unit

      def pointer(topic: Topic, latency: Long) = unit
    }

    def empty[F[_] : Applicative]: Metrics[F] = empty(Applicative[F].unit)
  }
}
