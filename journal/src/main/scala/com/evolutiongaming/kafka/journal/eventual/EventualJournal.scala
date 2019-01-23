package com.evolutiongaming.kafka.journal.eventual

import cats._
import cats.effect.Clock
import cats.implicits._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.skafka.Topic

trait EventualJournal[F[_]] {

  def pointers(topic: Topic): F[TopicPointers]

  def read(key: Key, from: SeqNr): Stream[F, ReplicatedEvent]

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

    def read(key: Key, from: SeqNr) = new Stream[F, ReplicatedEvent] {

      def foldWhileM[L, R](l: L)(f: (L, ReplicatedEvent) => F[Either[L, R]]) = {
        for {
          rl     <- Latency { journal.read(key, from).foldWhileM(l)(f) }
          (r, l)  = rl
          _      <- log.debug(s"$key read in ${ l }ms, from: $from, result: $r")
        } yield r
      }
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

    def read(key: Key, from: SeqNr) = {

      val stream = new Stream[F, ReplicatedEvent] {
        def foldWhileM[L, R](l: L)(f: (L, ReplicatedEvent) => F[Either[L, R]]) = {
          for {
            rl     <- Latency { journal.read(key, from).foldWhileM(l)(f) } // TODO around
            (r, l)  = rl
            _      <- metrics.read(topic = key.topic, latency = l)
          } yield r
        }
      }

      for {
        a <- stream
        _ <- Stream.lift(metrics.read(key.topic))
      } yield a
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

    def read(key: Key, from: SeqNr) = Stream.empty[F, ReplicatedEvent]

    def pointer(key: Key) = none[Pointer].pure[F]
  }


  trait Metrics[F[_]] {

    def pointers(topic: Topic, latency: Long): F[Unit]

    def read(topic: Topic, latency: Long): F[Unit]

    def read(topic: Topic): F[Unit]

    def pointer(topic: Topic, latency: Long): F[Unit]
  }

  object Metrics {

    def empty[F[_]](unit: F[Unit]): Metrics[F] = new Metrics[F] {

      def pointers(topic: Topic, latency: Long) = unit

      def read(topic: Topic, latency: Long) = unit

      def read(topic: Topic) = unit

      def pointer(topic: Topic, latency: Long) = unit
    }

    def empty[F[_] : Applicative]: Metrics[F] = empty(Applicative[F].unit)
  }


  implicit class EventualJournalOps[F[_]](val self: EventualJournal[F]) extends AnyVal {

    def mapK[G[_]](to: F ~> G, from: G ~> F): EventualJournal[G] = new EventualJournal[G] {

      def pointers(topic: Topic) = to(self.pointers(topic))

      def read(key: Key, from1: SeqNr) = self.read(key, from1).mapK(to, from)

      def pointer(key: Key) = to(self.pointer(key))
    }

    def withLog(log: Log[F])(implicit flatMap: FlatMap[F], clock: Clock[F]): EventualJournal[F] = {
      EventualJournal[F](self, log)
    }

    def withMetrics(metrics: Metrics[F])(implicit flatMap: FlatMap[F], clock: Clock[F]): EventualJournal[F] = {
      EventualJournal(self, metrics)
    }
  }
}
