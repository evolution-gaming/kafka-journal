package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.datastax.driver.core.{BoundStatement, ConsistencyLevel, PreparedStatement, ResultSet}
import com.datastax.driver.core.policies.RetryPolicy
import com.evolutiongaming.cassandra.Session
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._

import scala.concurrent.ExecutionContext

// TODO rename
trait PrepareAndExecute {
  def prepare(query: String): Async[PreparedStatement]
  def execute(statement: BoundStatement): Async[ResultSet]
}


object PrepareAndExecute {

  def apply(session: Session, config: StatementConfig)(implicit ec: ExecutionContext): PrepareAndExecute = {
    new PrepareAndExecute {

      def prepare(query: String) = {
        session.prepare(query).async
      }

      def execute(statement: BoundStatement) = {
        val statementConfigured = statement.set(config)
        session.execute(statementConfigured).async
      }
    }
  }
}


final case class StatementConfig(
  idempotent: Boolean = false,
  consistencyLevel: ConsistencyLevel,
  retryPolicy: RetryPolicy)