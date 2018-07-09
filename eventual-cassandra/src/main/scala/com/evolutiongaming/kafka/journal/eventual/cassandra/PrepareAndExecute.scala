package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.datastax.driver.core._
import com.datastax.driver.core.policies.RetryPolicy
import com.evolutiongaming.cassandra.CassandraHelper._
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._

import scala.concurrent.Future

// TODO rename
trait PrepareAndExecute {
  def prepare(query: String): Future[PreparedStatement]
  def execute(statement: BoundStatement): Future[ResultSet]
}


object PrepareAndExecute {

  def apply(session: Session, statementConfig: StatementConfig): PrepareAndExecute = {
    new PrepareAndExecute {

      def prepare(query: String) = {
        session.prepareAsync(query).asScala()
      }

      def execute(statement: BoundStatement) = {
        val statementConfigured = statement.set(statementConfig)
        val result = session.executeAsync(statementConfigured)
        result.asScala()
      }
    }
  }
}


case class StatementConfig(
  idempotent: Boolean = false,
  consistencyLevel: ConsistencyLevel,
  retryPolicy: RetryPolicy)