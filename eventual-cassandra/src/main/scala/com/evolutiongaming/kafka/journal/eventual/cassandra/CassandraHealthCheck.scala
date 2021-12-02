package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Monad
import cats.effect._
import cats.syntax.all._
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper.{Log, LogOf, Schedule}
import com.evolutiongaming.kafka.journal.util.CatsHelper._
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig.ConsistencyConfig

import scala.concurrent.duration._
import cats.effect.{ Ref, Temporal }

trait CassandraHealthCheck[F[_]] {
  def error: F[Option[Throwable]]
}

object CassandraHealthCheck {

  def of[F[_] : Concurrent : Timer : LogOf](
    session: Resource[F, CassandraSession[F]],
    consistencyConfig: ConsistencyConfig.Read
  ): Resource[F, CassandraHealthCheck[F]] = {

    val statement = for {
      session   <- session
      statement <- {
        implicit val session1 = session
        Statement.of[F](consistencyConfig).toResource
      }
    } yield statement

    for {
      log    <- LogOf[F].apply(CassandraHealthCheck.getClass).toResource
      result <- of(initial = 10.seconds, interval = 1.second, statement = statement, log = log)
    } yield result
  }

  def of[F[_] : Concurrent : Temporal](
    initial: FiniteDuration,
    interval: FiniteDuration,
    statement: Resource[F, Statement[F]],
    log: Log[F]
  ): Resource[F, CassandraHealthCheck[F]] = {

    for {
      ref       <- Ref.of[F, Option[Throwable]](none).toResource
      statement <- statement
      _         <- Schedule(initial, interval) {
        for {
          e <- statement.error[Throwable]
          _ <- e.foldMapM { e => log.error(s"failed with $e", e) }
          _ <- ref.set(e)
        } yield {}
      }
    } yield {
      new CassandraHealthCheck[F] {
        def error = ref.get
      }
    }
  }


  type Statement[F[_]] = F[Unit]

  object Statement {

    def of[F[_] : Monad : CassandraSession](consistency: ConsistencyConfig.Read): F[Statement[F]] = {
      for {
        prepared <- "SELECT now() FROM system.local".prepare
      } yield {
        prepared.bind().setConsistencyLevel(consistency.value).first.void
      }
    }
  }
}
