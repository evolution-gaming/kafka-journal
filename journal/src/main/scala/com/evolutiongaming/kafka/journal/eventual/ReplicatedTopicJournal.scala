package com.evolutiongaming.kafka.journal.eventual

import java.time.Instant

import cats.data.{NonEmptyMap => Nem}
import cats.effect.Resource
import cats.implicits._
import cats.{Applicative, Defer, Monad, ~>}
import com.evolutiongaming.catshelper.{ApplicativeThrowable, BracketThrowable, Log}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.skafka.{Offset, Partition, Topic}
import com.evolutiongaming.smetrics._


trait ReplicatedTopicJournal[F[_]] {

  def pointers: F[TopicPointers]

  def journal(id: String): Resource[F, ReplicatedKeyJournal[F]]

  def save(pointers: Nem[Partition, Offset], timestamp: Instant): F[Unit]
}

object ReplicatedTopicJournal {

  def empty[F[_] : Applicative]: ReplicatedTopicJournal[F] = new ReplicatedTopicJournal[F] {

    def pointers = TopicPointers.empty.pure[F]

    def journal(id: String) = {
      Resource.liftF(ReplicatedKeyJournal.empty[F].pure[F])
    }

    def save(pointers: Nem[Partition, Offset], timestamp: Instant) = ().pure[F]
  }


  def apply[F[_] : Applicative](
    topic: Topic,
    replicatedJournal: ReplicatedJournalFlat[F]
  ): ReplicatedTopicJournal[F] = {

    new ReplicatedTopicJournal[F] {

      def pointers = replicatedJournal.pointers(topic)

      def journal(id: String) = {
        val key = Key(id = id, topic = topic)
        val replicatedKeyJournal = ReplicatedKeyJournal(key, replicatedJournal)
        Resource.liftF(replicatedKeyJournal.pure[F])
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
      F: ApplicativeThrowable[F]
    ): ReplicatedTopicJournal[F] = {

      def error[A](msg: String, cause: Throwable) = {
        JournalError(s"ReplicatedTopicJournal.$msg failed with $cause", cause).raiseError[F, A]
      }

      new ReplicatedTopicJournal[F] {

        def pointers = {
          self
            .pointers
            .handleErrorWith { a => error(s"pointers topic: $topic", a) }
        }

        def journal(id: String) = {
          self
            .journal(id)
            .map { _.enhanceError(Key(id = id, topic = topic)) }
        }

        def save(pointers: Nem[Partition, Offset], timestamp: Instant) = {
          self
            .save(pointers, timestamp)
            .handleErrorWith { a =>
              error(s"save " +
                s"topic: $topic, " +
                s"pointers: $pointers, " +
                s"timestamp: $timestamp", a)
            }
        }
      }
    }
  }
}