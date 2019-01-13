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

  def apply[F[_]](implicit F: EventualJournal[F]): EventualJournal[F] = F

  def apply[F[_] : FlatMap : Clock](journal: EventualJournal[F], log: Log[F]): EventualJournal[F] = new EventualJournal[F] {

    def pointers(topic: Topic) = {
      for {
        rl     <- Latency { journal.pointers(topic) }
        (r, l)  = rl
        _      <- log.debug(s"$topic pointers in ${ l }ms, result: $r")
      } yield r
    }

    def read[S](key: Key, from: SeqNr, s: S)(f: Fold[S, ReplicatedEvent]) = {
      for {
        rl     <- Latency { journal.read(key, from, s)(f) }
        (r, l)  = rl
        _      <- log.debug(s"$key read in ${ l }ms, from: $from, state: $s, result: $r")
      } yield r
    }

    def pointer(key: Key) = {
      for {
        rl     <- Latency { journal.pointer(key) }
        (r, l)  = rl
        _      <- log.debug(s"$key pointer in ${ l }ms, result: $r")
      } yield r
    }
  }


  def apply[F[_] : FlatMap : Clock](
    journal: EventualJournal[F],
    metrics: Metrics[F]): EventualJournal[F] = new EventualJournal[F] {

    def pointers(topic: Topic) = {
      for {
        rl     <- Latency { journal.pointers(topic) }
        (r, l)  = rl
        _      <- metrics.pointers(topic, l)
      } yield r
    }

    def read[S](key: Key, from: SeqNr, s: S)(f: Fold[S, ReplicatedEvent]) = {
      val ff: Fold[(S, Int), ReplicatedEvent] = {
        case ((s, n), e) => f(s, e).map { s => (s, n + 1) }
      }
      for {
        rl      <- Latency { journal.read(key, from, (s, 0))(ff) }
        (s, l)   = rl
        (_, es)  = s.s
        _       <- metrics.read(topic = key.topic, latency = l, events = es)
        r        = s.map { case (s, _) => s }
      } yield r
    }

    def pointer(key: Key) = {
      for {
        rl     <- Latency { journal.pointer(key) }
        (r, l)  = rl
        _      <- metrics.pointer(key.topic, l)
      } yield r
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


  implicit class EventualJournalOps[F[_]](val self: EventualJournal[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): EventualJournal[G] = new EventualJournal[G] {

      def pointers(topic: Topic) = f(self.pointers(topic))

      def read[S](key: Key, from: SeqNr, s: S)(f1: Fold[S, ReplicatedEvent]) = {
        f(self.read[S](key, from, s)(f1))
      }

      def pointer(key: Key) = f(self.pointer(key))
    }

    def withLog(log: Log[F])(implicit flatMap: FlatMap[F], clock: Clock[F]): EventualJournal[F] = {
      EventualJournal[F](self, log)
    }

    def withMetrics(metrics: Metrics[F])(implicit flatMap: FlatMap[F], clock: Clock[F]): EventualJournal[F] = {
      EventualJournal(self, metrics)
    }
  }
}
