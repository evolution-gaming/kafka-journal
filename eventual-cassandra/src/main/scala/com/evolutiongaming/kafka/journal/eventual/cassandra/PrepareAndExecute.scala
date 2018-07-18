package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.datastax.driver.core._
import com.datastax.driver.core.policies.RetryPolicy
import com.evolutiongaming.cassandra.CassandraHelper._
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

  def apply(
    session: Session,
    statementConfig: StatementConfig)(implicit ec: ExecutionContext): PrepareAndExecute = {

    new PrepareAndExecute {

      def prepare(query: String) = {
        session.prepareAsync(query).asScala().async
      }

      def execute(statement: BoundStatement) = {
        val statementConfigured = statement.set(statementConfig)
        val result = session.executeAsync(statementConfigured)
        result.asScala().async
      }
    }
  }
}


case class StatementConfig(
  idempotent: Boolean = false,
  consistencyLevel: ConsistencyLevel,
  retryPolicy: RetryPolicy)