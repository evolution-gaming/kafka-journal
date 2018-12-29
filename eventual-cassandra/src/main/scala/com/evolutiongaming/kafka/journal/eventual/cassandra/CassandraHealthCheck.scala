package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.FlatMap
import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._
import com.evolutiongaming.kafka.journal.Log
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.kafka.journal.util.{FromFuture, TimerOf}
import com.evolutiongaming.scassandra.Session

import scala.concurrent.duration._

trait CassandraHealthCheck[F[_]] {

  def error: F[Option[Throwable]]

  // TODO fiber
  def close: F[Unit]
}

object CassandraHealthCheck {

  def of[F[_] : Concurrent : Timer : FromFuture](session: Session, retries: Int): F[CassandraHealthCheck[F]] = {
    implicit val cassandraSession = CassandraSession(CassandraSession[F](session), retries)
    for {
      log       <- Log.of[F](CassandraHealthCheck.getClass)
      statement <- Statement.of[F]
      result    <- {
        implicit val log1 = log
        of[F](statement, 1.second, CassandraSession[F].close)
      }
    } yield {
      result
    }
  }

  def of[F[_] : Concurrent : Log : Timer](
    statement: Statement[F],
    interval: FiniteDuration,
    close: F[Unit]): F[CassandraHealthCheck[F]] = {

    for {
      ref   <- Ref.of[F, Option[Throwable]](none)
      fiber <- Concurrent[F].start {
        Sync[F].guarantee {
          for {
            _ <- TimerOf[F].sleep(10.seconds)
            _ <- {
              for {
                error <- Sync[F].attempt(statement) // TODO redeem
                _     <- error.fold(error => Log[F].error(s"failed with $error"), _.pure[F])
                _     <- ref.set(error.fold(_.some, _ => none))
                _     <- TimerOf[F].sleep(interval)
              } yield ().asLeft
            }.foreverM[Unit]
          } yield {}
        } {
          close
        }
      }
    } yield {
      new CassandraHealthCheck[F] {

        def error = ref.get

        def close = fiber.cancel
      }
    }
  }


  type Statement[F[_]] = F[Unit]

  object Statement {

    def of[F[_] : FlatMap : CassandraSession]: F[Statement[F]] = {
      for {
        prepared <- "SELECT now() FROM system.local".prepare
      } yield {
        prepared.bind().execute.void
      }
    }
  }
}
