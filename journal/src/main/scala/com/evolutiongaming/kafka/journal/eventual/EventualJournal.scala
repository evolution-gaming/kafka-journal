package com.evolutiongaming.kafka.journal.eventual

import cats.Applicative
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.AsyncImplicits._
import com.evolutiongaming.kafka.journal.FoldWhile._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.Topic

trait EventualJournal {

  def pointers(topic: Topic): Async[TopicPointers]

  def read[S](key: Key, from: SeqNr, s: S)(f: Fold[S, ReplicatedEvent]): Async[Switch[S]]

  // TODO not Use Pointer until tested
  def pointer(key: Key): Async[Option[Pointer]]
}

object EventualJournal {

  val Empty: EventualJournal = new EventualJournal {

    def pointers(topic: Topic) = TopicPointers.Empty.async

    def read[S](key: Key, from: SeqNr, state: S)(f: Fold[S, ReplicatedEvent]) = state.continue.async

    def pointer(key: Key) = Async.none
  }


  def apply(journal: EventualJournal, log: ActorLog): EventualJournal = new EventualJournal {

    def pointers(topic: Topic) = {
      for {
        tuple <- Latency { journal.pointers(topic) }
        (result, latency) = tuple
        _ = log.debug(s"$topic pointers in ${ latency }ms, result: $result")
      } yield result
    }

    def read[S](key: Key, from: SeqNr, s: S)(f: Fold[S, ReplicatedEvent]) = {
      for {
        tuple <- Latency { journal.read(key, from, s)(f) }
        (result, latency) = tuple
        _ = log.debug(s"$key read in ${ latency }ms, from: $from, state: $s, result: $result")
      } yield result
    }

    def pointer(key: Key) = {
      for {
        tuple <- Latency { journal.pointer(key) }
        (result, latency) = tuple
        _ = log.debug(s"$key pointer in ${ latency }ms, result: $result")
      } yield result
    }
  }


  def apply(journal: EventualJournal, metrics: Metrics[Async]): EventualJournal = new EventualJournal {

    def pointers(topic: Topic) = {
      for {
        tuple <- Latency { journal.pointers(topic) }
        (result, latency) = tuple
        _ <- metrics.pointers(topic, latency)
      } yield result
    }

    def read[S](key: Key, from: SeqNr, s: S)(f: Fold[S, ReplicatedEvent]) = {
      val ff: Fold[(S, Int), ReplicatedEvent] = {
        case ((s, n), e) => f(s, e).map { s => (s, n + 1) }
      }
      for {
        tuple <- Latency { journal.read(key, from, (s, 0))(ff) }
        (switch, latency) = tuple
        (_, events) = switch.s
        _ <- metrics.read(topic = key.topic, latency = latency, events = events)
        result = switch.map { case (s, _) => s }
      } yield result
    }

    def pointer(key: Key) = {
      for {
        tuple <- Latency { journal.pointer(key) }
        (result, latency) = tuple
        _ <- metrics.pointer(key.topic, latency)
      } yield result
    }
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
