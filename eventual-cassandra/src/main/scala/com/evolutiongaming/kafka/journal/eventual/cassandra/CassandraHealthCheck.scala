package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Monad
import cats.effect._
import com.evolutiongaming.catshelper.{Log, LogOf}
import com.evolutiongaming.kafka.journal.cassandra.{CassandraHealthCheck => CassandraHealthCheck2}
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig.ConsistencyConfig

import scala.concurrent.duration._

@deprecated(since = "3.3.9", message = "Use a class from `com.evolutiongaming.kafka.journal.cassandra` (without `eventual` part) package instead")
trait CassandraHealthCheck[F[_]] {
  def error: F[Option[Throwable]]
}

@deprecated(since = "3.3.9", message = "Use a class from `com.evolutiongaming.kafka.journal.cassandra` (without `eventual` part) package instead")
object CassandraHealthCheck {

  private[cassandra] def apply[F[_]](cassandraHealthCheck2: CassandraHealthCheck2[F]): CassandraHealthCheck[F] =
    new CassandraHealthCheck[F] {
      def error: F[Option[Throwable]] = cassandraHealthCheck2.error
    }

  def of[F[_] : Temporal : LogOf](
    session: Resource[F, CassandraSession[F]],
    consistencyConfig: ConsistencyConfig.Read
  ): Resource[F, CassandraHealthCheck[F]] =
    CassandraHealthCheck2
    .of(session, consistencyConfig.toCassandraConsistencyConfig)
    .map(CassandraHealthCheck(_))

  def of[F[_] : Temporal](
    initial: FiniteDuration,
    interval: FiniteDuration,
    statement: Resource[F, Statement[F]],
    log: Log[F]
  ): Resource[F, CassandraHealthCheck[F]] = 
    CassandraHealthCheck2
    .of(initial = initial, interval = interval, statement = statement, log = log)
    .map(CassandraHealthCheck(_))


  type Statement[F[_]] = CassandraHealthCheck2.Statement[F]

  object Statement {

    def of[F[_] : Monad : CassandraSession](consistency: ConsistencyConfig.Read): F[Statement[F]] =
      CassandraHealthCheck2.Statement.of(consistency.toCassandraConsistencyConfig)

  }
}
