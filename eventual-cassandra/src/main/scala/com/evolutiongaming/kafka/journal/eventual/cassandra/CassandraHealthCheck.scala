package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.FlatMap
import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._
import com.evolutiongaming.kafka.journal.Log
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.kafka.journal.util.CatsHelper._

import scala.concurrent.duration._

trait CassandraHealthCheck[F[_]] {
  def error: F[Option[Throwable]]
}

object CassandraHealthCheck {

  def of[F[_] : Concurrent : Timer : ContextShift](
    session: Resource[F, CassandraSession[F]]): Resource[F, CassandraHealthCheck[F]] = {

    val statement = for {
      session   <- session
      statement <- {
        implicit val session1 = session
        Resource.liftF(Statement.of[F])
      }
    } yield statement

    for {
      log    <- Resource.liftF(Log.of[F](CassandraHealthCheck.getClass))
      result <- {
        implicit val log1 = log
        of(initial = 10.seconds, interval = 1.second, statement = statement)
      }
    } yield result
  }

  def of[F[_] : Concurrent : Timer : ContextShift : Log](
    initial: FiniteDuration,
    interval: FiniteDuration,
    statement: Resource[F, Statement[F]]): Resource[F, CassandraHealthCheck[F]] = {

    Resource {
      for {
        ref   <- Ref.of[F, Option[Throwable]](none)
        fiber <- statement.start { statement =>
          for {
            _ <- Timer[F].sleep(initial)
            _ <- {
              for {
                e <- statement.toError[Throwable]
                _ <- e.fold(().pure[F]) { e => Log[F].error(s"failed with $e", e) }
                _ <- ref.set(e)
                _ <- Timer[F].sleep(interval)
              } yield ().asLeft
            }.foreverM[Unit]
          } yield {}
        }
      } yield {
        val result = new CassandraHealthCheck[F] {
          def error = ref.get
        }
        (result, fiber.cancel)
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
