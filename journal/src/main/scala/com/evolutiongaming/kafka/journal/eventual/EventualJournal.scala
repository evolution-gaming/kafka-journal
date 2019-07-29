package com.evolutiongaming.kafka.journal.eventual

import cats._
import cats.arrow.FunctionK
import cats.implicits._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.smetrics.MeasureDuration
import com.evolutiongaming.sstream.Stream

import scala.concurrent.duration.FiniteDuration

trait EventualJournal[F[_]] {

  def pointers(topic: Topic): F[TopicPointers]

  def read(key: Key, from: SeqNr): Stream[F, EventRecord]

  // TODO not Use Pointer until tested
  def pointer(key: Key): F[Option[Pointer]]
}

object EventualJournal {

  def apply[F[_]](implicit F: EventualJournal[F]): EventualJournal[F] = F

  def apply[F[_] : FlatMap : MeasureDuration](journal: EventualJournal[F], log: Log[F]): EventualJournal[F] = {

    val functionKId = FunctionK.id[F]

    new EventualJournal[F] {

      def pointers(topic: Topic) = {
        for {
          d <- MeasureDuration[F].start
          r <- journal.pointers(topic)
          d <- d
          _ <- log.debug(s"$topic pointers in ${ d.toMillis }ms, result: $r")
        } yield r
      }

      def read(key: Key, from: SeqNr) = {
        val logging = new (F ~> F) {
          def apply[A](fa: F[A]) = {
            for {
              d <- MeasureDuration[F].start
              r <- fa
              d <- d
              _ <- log.debug(s"$key read in ${ d.toMillis }ms, from: $from, result: $r")
            } yield r
          }
        }
        journal.read(key, from).mapK(logging, functionKId)
      }

      def pointer(key: Key) = {
        for {
          d <- MeasureDuration[F].start
          r <- journal.pointer(key)
          d <- d
          _ <- log.debug(s"$key pointer in ${ d.toMillis }ms, result: $r")
        } yield r
      }
    }
  }


  def apply[F[_] : FlatMap : MeasureDuration](
    journal: EventualJournal[F],
    metrics: Metrics[F]): EventualJournal[F] = {

    val functionKId = FunctionK.id[F]

    new EventualJournal[F] {

      def pointers(topic: Topic) = {
        for {
          d <- MeasureDuration[F].start
          r <- journal.pointers(topic)
          d <- d
          _ <- metrics.pointers(topic, d)
        } yield r
      }

      def read(key: Key, from: SeqNr) = {
        val measure = new (F ~> F) {
          def apply[A](fa: F[A]) = {
            for {
              d <- MeasureDuration[F].start
              r <- fa
              d <- d
              _ <- metrics.read(topic = key.topic, latency = d)
            } yield r
          }
        }

        for {
          a <- journal.read(key, from).mapK(measure, functionKId)
          _ <- Stream.lift(metrics.read(key.topic))
        } yield a
      }

      def pointer(key: Key) = {
        for {
          d <- MeasureDuration[F].start
          r <- journal.pointer(key)
          d <- d
          _ <- metrics.pointer(key.topic, d)
        } yield r
      }
    }
  }


  def empty[F[_] : Applicative]: EventualJournal[F] = new EventualJournal[F] {

    def pointers(topic: Topic) = TopicPointers.Empty.pure[F]

    def read(key: Key, from: SeqNr) = Stream.empty[F, EventRecord]

    def pointer(key: Key) = none[Pointer].pure[F]
  }


  trait Metrics[F[_]] {

    def pointers(topic: Topic, latency: FiniteDuration): F[Unit]

    def read(topic: Topic, latency: FiniteDuration): F[Unit]

    def read(topic: Topic): F[Unit]

    def pointer(topic: Topic, latency: FiniteDuration): F[Unit]
  }

  object Metrics {

    def empty[F[_]](unit: F[Unit]): Metrics[F] = new Metrics[F] {

      def pointers(topic: Topic, latency: FiniteDuration) = unit

      def read(topic: Topic, latency: FiniteDuration) = unit

      def read(topic: Topic) = unit

      def pointer(topic: Topic, latency: FiniteDuration) = unit
    }

    def empty[F[_] : Applicative]: Metrics[F] = empty(Applicative[F].unit)
  }


  implicit class EventualJournalOps[F[_]](val self: EventualJournal[F]) extends AnyVal {

    def mapK[G[_]](to: F ~> G, from: G ~> F): EventualJournal[G] = new EventualJournal[G] {

      def pointers(topic: Topic) = to(self.pointers(topic))

      def read(key: Key, from1: SeqNr) = self.read(key, from1).mapK(to, from)

      def pointer(key: Key) = to(self.pointer(key))
    }

    def withLog(log: Log[F])(implicit F: FlatMap[F], measureDuration: MeasureDuration[F]): EventualJournal[F] = {
      EventualJournal[F](self, log)
    }

    def withMetrics(metrics: Metrics[F])(implicit F: FlatMap[F], measureDuration: MeasureDuration[F]): EventualJournal[F] = {
      EventualJournal(self, metrics)
    }
  }
}
