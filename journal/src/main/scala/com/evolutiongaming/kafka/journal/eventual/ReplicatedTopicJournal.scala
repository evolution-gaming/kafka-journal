package com.evolutiongaming.kafka.journal.eventual

import cats.data.NonEmptyMap._
import cats.data.{NonEmptyMap => Nem}
import cats.effect.Resource
import cats.effect.syntax.all._
import cats.syntax.all._
import cats.{Applicative, Defer, Monad, ~>}
import com.evolutiongaming.catshelper.{BracketThrowable, Log, MonadThrowable}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.skafka.{Offset, Partition, Topic}
import com.evolutiongaming.smetrics._

import java.time.Instant


trait ReplicatedTopicJournal[F[_]] {
  import ReplicatedTopicJournal._

  def pointers: F[TopicPointers]

  def journal(id: String): Resource[F, ReplicatedKeyJournal[F]]

  def save(pointers: Nem[Partition, Offset], timestamp: Instant): F[Changed]
}

object ReplicatedTopicJournal {

  type Changed = Boolean


  def empty[F[_] : Applicative]: ReplicatedTopicJournal[F] = new ReplicatedTopicJournal[F] {

    def pointers = TopicPointers.empty.pure[F]

    def journal(id: String) = {
      ReplicatedKeyJournal
        .empty[F]
        .pure[F]
        .toResource
    }

    def save(pointers: Nem[Partition, Offset], timestamp: Instant) = false.pure[F]
  }


  def apply[F[_] : Applicative](
    topic: Topic,
    replicatedJournal: ReplicatedJournalFlat[F]
  ): ReplicatedTopicJournal[F] = {

    new ReplicatedTopicJournal[F] {

      def pointers = replicatedJournal.pointers(topic)

      def journal(id: String) = {
        val key = Key(id = id, topic = topic)
        ReplicatedKeyJournal(key, replicatedJournal)
          .pure[F]
          .toResource
      }

      def save(pointers: Nem[Partition, Offset], timestamp: Instant) = {
        replicatedJournal.save(topic, pointers, timestamp)
      }
    }
  }


  implicit class ReplicatedTopicJournalOps[F[_]](val self: ReplicatedTopicJournal[F]) extends AnyVal {

    def mapK[G[_]](
      f: F ~> G)(implicit
      B: BracketThrowable[F],
      GT: BracketThrowable[G],
      D: Defer[G],
      G: Applicative[G]
    ): ReplicatedTopicJournal[G] = new ReplicatedTopicJournal[G] {

      def pointers = f(self.pointers)

      def journal(id: String) = {
        self
          .journal(id)
          .map(_.mapK(f))
          .mapK(f)
      }

      def save(pointers: Nem[Partition, Offset], timestamp: Instant) = {
        f(self.save(pointers, timestamp))
      }
    }


    def withLog(
      topic: Topic,
      log: Log[F])(implicit
      F: Monad[F],
      measureDuration: MeasureDuration[F]
    ): ReplicatedTopicJournal[F] = {

      new ReplicatedTopicJournal[F] {

        def pointers = {
          for {
            d <- MeasureDuration[F].start
            r <- self.pointers
            d <- d
            _ <- log.debug(s"$topic pointers in ${ d.toMillis }ms, result: $r")
          } yield r
        }

        def journal(id: String) = {
          self
            .journal(id)
            .map { _.withLog(Key(id = id, topic = topic), log) }
        }

        def save(pointers: Nem[Partition, Offset], timestamp: Instant) = {
          for {
            d <- MeasureDuration[F].start
            r <- self.save(pointers, timestamp)
            d <- d
            _ <- log.debug(s"$topic save in ${ d.toMillis }ms, pointers: ${ pointers.mkString_(",") }, timestamp: $timestamp")
          } yield r
        }
      }
    }


    def withMetrics(
      topic: Topic,
      metrics: ReplicatedJournal.Metrics[F])(implicit
      F: Monad[F],
      measureDuration: MeasureDuration[F]
    ): ReplicatedTopicJournal[F] = {
      new ReplicatedTopicJournal[F] {

        def pointers = {
          for {
            d <- MeasureDuration[F].start
            r <- self.pointers
            d <- d
            _ <- metrics.pointers(d)
          } yield r
        }

        def journal(id: String) = {
          self
            .journal(id)
            .map { _.withMetrics(topic, metrics) }
        }

        def save(pointers: Nem[Partition, Offset], timestamp: Instant) = {
          for {
            d <- MeasureDuration[F].start
            r <- self.save(pointers, timestamp)
            d <- d
            _ <- metrics.save(topic, d)
          } yield r
        }
      }
    }


    def enhanceError(
      topic: Topic)(implicit
      F: MonadThrowable[F]
    ): ReplicatedTopicJournal[F] = {

      def journalError[A](msg: String, cause: Throwable) = {
        JournalError(s"ReplicatedTopicJournal.$msg failed with $cause", cause)
      }

      new ReplicatedTopicJournal[F] {

        def pointers = {
          self
            .pointers
            .adaptError { case a => journalError(s"pointers topic: $topic", a) }
        }

        def journal(id: String) = {
          val key = Key(id = id, topic = topic)
          self
            .journal(id)
            .map { _.enhanceError(key) }
            .adaptError { case a => journalError(s"journal key: $key", a) }
        }

        def save(pointers: Nem[Partition, Offset], timestamp: Instant) = {
          self
            .save(pointers, timestamp)
            .adaptError { case a =>
              journalError(s"save " +
                s"topic: $topic, " +
                s"pointers: $pointers, " +
                s"timestamp: $timestamp", a)
            }
        }
      }
    }
  }
}