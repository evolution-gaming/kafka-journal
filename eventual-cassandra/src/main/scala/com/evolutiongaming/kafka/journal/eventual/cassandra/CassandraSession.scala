package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.datastax.driver.core._
import com.datastax.driver.core.policies.{LoggingRetryPolicy, RetryPolicy}
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.kafka.journal.{FromFuture, IO2}
import com.evolutiongaming.scassandra.{NextHostRetryPolicy, Session}

trait CassandraSession[F[_]] {

  def prepare(query: String): F[PreparedStatement]

  def execute(statement: Statement): F[ResultSet]

  final def execute(statement: String): F[ResultSet] = execute(new SimpleStatement(statement))
}


object CassandraSession {

  def apply[F[_]](implicit F: CassandraSession[F]): CassandraSession[F] = F

  def apply[F[_] : IO2 : FromFuture](
    session: Session,
    retries: Int,
    trace: Boolean = false): CassandraSession[F] = {

    val retryPolicy = new LoggingRetryPolicy(NextHostRetryPolicy(retries))
    apply(session, retryPolicy, trace)
  }

  def apply[F[_] : IO2 : FromFuture](
    session: Session,
    retryPolicy: RetryPolicy,
    trace: Boolean): CassandraSession[F] = {

    new CassandraSession[F] {

      def prepare(query: String) = {
        IO2[F].from {
          session.prepare(query)
        }
      }

      def execute(statement: Statement) = {
        val configured = statement
          .setRetryPolicy(retryPolicy)
          .setIdempotent(true)
          .trace(trace)

        IO2[F].from {
          session.execute(configured)
        }
      }
    }
  }
}