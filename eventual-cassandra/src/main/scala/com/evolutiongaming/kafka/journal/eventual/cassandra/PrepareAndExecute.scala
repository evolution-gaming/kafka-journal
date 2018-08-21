package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.datastax.driver.core._
import com.datastax.driver.core.policies.RetryPolicy
import com.evolutiongaming.cassandra.Session
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._

import scala.concurrent.ExecutionContext

// TODO rename
trait PrepareAndExecute {
  def prepare(query: String): Async[PreparedStatement]
  // TODO remove duplicate and wrap
  def execute(statement: BoundStatement): Async[ResultSet]
  def execute(statement: BatchStatement): Async[ResultSet]
}


object PrepareAndExecute {

  def apply(session: Session, config: StatementConfig)(implicit ec: ExecutionContext): PrepareAndExecute = {
    new PrepareAndExecute {

      def prepare(query: String) = session.prepare(query).async

      def execute(statement: BoundStatement) = exec(statement)

      def execute(statement: BatchStatement) = exec(statement)

      private def exec(statement: Statement) = {
        val statementConfigured = statement.set(config)
        session.execute(statementConfigured).async
      }
    }
  }
}


final case class StatementConfig(
  idempotent: Boolean = false, // TODO!!!!
  consistencyLevel: ConsistencyLevel,
  retryPolicy: RetryPolicy)