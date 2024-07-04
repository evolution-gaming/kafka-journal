package com.evolutiongaming.kafka.journal.cassandra

import cats.Monad
import cats.effect._
import cats.effect.syntax.resource._
import cats.syntax.all._
import com.evolutiongaming.catshelper.{Log, LogOf, Schedule}
import com.evolutiongaming.kafka.journal.cassandra.CassandraConsistencyConfig
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraSession
import com.evolutiongaming.kafka.journal.util.CatsHelper._

import scala.concurrent.duration._

/** Performs a check if Cassandra is alive.
  *
  * The common implementation is to periodically do a simple query and check
  * if it returns an error.
  */
@deprecated(since = "3.3.10", message = "Use `com.evolutiongaming.scassandra.CassandraHealthCheck` instead")
trait CassandraHealthCheck[F[_]] {

  /** @return `None` if Cassandra healthy, and `Some(error)` otherwise */
  def error: F[Option[Throwable]]

}

object CassandraHealthCheck {

  /** Checks if Cassandra is alive by requesting a current timestamp from Cassandra.
    *
    * I.e., it does the following query every second after initial ramp-up delay of 10 seconds.
    * ```sql
    * SELECT now() FROM system.local
    * ```
    *
    * @param session
    *   Cassandra session factory to use to perform queries with.
    * @param consistencyConfig
    *   Read consistency level to use for a query.
    *
    * @return
    *   Factory for `CassandraHealthCheck` instances.
    */
  def of[F[_] : Temporal : LogOf](
    session: Resource[F, CassandraSession[F]],
    consistencyConfig: CassandraConsistencyConfig.Read
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

  /** Checks if server is alive by doing a custom `F[Unit]` call.
    *
    * @param initial
    *   Initial ramp-up delay before health checks are started.
    * @param interval
    *   How often the provided function should be called.
    * @param statement
    *   The function to call to check if server is alive. The function is expected to throw an error if server is not
    *   healthy.
    * @param log
    *   The log to write an error to, in addition to throwing an error in [[CassandraHealthCheck#error]] call.
    *
    * @return
    *   Factory for `CassandraHealthCheck` instances.
    */
  def of[F[_] : Temporal](
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

    def of[F[_] : Monad : CassandraSession](consistency: CassandraConsistencyConfig.Read): F[Statement[F]] = {
      for {
        prepared <- "SELECT now() FROM system.local".prepare
      } yield {
        prepared.bind().setConsistencyLevel(consistency.value).first.void
      }
    }
  }
}
