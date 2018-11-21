package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.datastax.driver.core._
import com.datastax.driver.core.policies.{LoggingRetryPolicy, RetryPolicy}
import com.evolutiongaming.kafka.journal.{FromFuture, IO}
import com.evolutiongaming.scassandra.{NextHostRetryPolicy, Session}

trait CassandraSession[F[_]] {

  def prepare(query: String): F[PreparedStatement]

  def execute(statement: Statement): F[ResultSet]

  final def execute(statement: String): F[ResultSet] = execute(new SimpleStatement(statement))
}


object CassandraSession {

  def apply[F[_]](implicit F: CassandraSession[F]): CassandraSession[F] = F

  def apply[F[_] : IO : FromFuture](session: Session, retries: Int): CassandraSession[F] = {
    val retryPolicy = new LoggingRetryPolicy(NextHostRetryPolicy(retries))
    apply(session, retryPolicy)
  }

  def apply[F[_] : IO : FromFuture](session: Session, retryPolicy: RetryPolicy): CassandraSession[F] = {
    new CassandraSession[F] {

      def prepare(query: String) = {
        IO[F].from {
          session.prepare(query)
        }
      }

      def execute(statement: Statement) = {
        val statementConfigured = statement
          .setRetryPolicy(retryPolicy)
          .setIdempotent(true)
        IO[F].from {
          session.execute(statementConfigured)
        }
      }
    }
  }
}