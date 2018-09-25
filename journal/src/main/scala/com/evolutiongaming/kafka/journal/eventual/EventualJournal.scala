package com.evolutiongaming.kafka.journal.eventual

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.AsyncHelper._
import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.kafka.journal.{Key, Latency, ReplicatedEvent, SeqNr}
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.Topic

trait EventualJournal {

  def pointers(topic: Topic): Async[TopicPointers]

  def read[S](key: Key, from: SeqNr, s: S)(f: Fold[S, ReplicatedEvent]): Async[Switch[S]]

  def lastSeqNr(key: Key, from: SeqNr): Async[Option[SeqNr]]
}

object EventualJournal {

  val Empty: EventualJournal = new EventualJournal {

    def pointers(topic: Topic) = TopicPointers.Empty.async

    def read[S](key: Key, from: SeqNr, state: S)(f: Fold[S, ReplicatedEvent]) = state.continue.async

    def lastSeqNr(key: Key, from: SeqNr) = Async.none
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

    def lastSeqNr(key: Key, from: SeqNr) = {
      for {
        tuple <- Latency { journal.lastSeqNr(key, from) }
        (result, latency) = tuple
        _ = log.debug(s"$key lastSeqNr in ${ latency }ms, from: $from, result: $result")
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

    def lastSeqNr(key: Key, from: SeqNr) = {
      for {
        tuple <- Latency { journal.lastSeqNr(key, from) }
        (result, latency) = tuple
        _ <- metrics.pointers(key.topic, latency)
      } yield result
    }
  }

  trait Metrics[F[_]] {

    def pointers(topic: Topic, latency: Long): F[Unit]

    def read(topic: Topic, latency: Long, events: Int): F[Unit]

    def lastSeqNr(topic: Topic, latency: Long): F[Unit]
  }

  object Metrics {

    def empty[F[_]](unit: F[Unit]): Metrics[F] = new Metrics[F] {

      def pointers(topic: Topic, latency: Long) = unit

      def read(topic: Topic, latency: Long, events: Int) = unit

      def lastSeqNr(topic: Topic, latency: Long) = unit
    }
  }
}
