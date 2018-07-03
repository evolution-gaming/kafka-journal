package com.evolutiongaming.kafka.journal.ally.cassandra

import com.datastax.driver.core.{BoundStatement, PreparedStatement, ResultSet}

import scala.concurrent.Future

// TODO rename
trait PrepareAndExecute {
  def prepare(query: String): Future[PreparedStatement]
  def execute(statement: BoundStatement): Future[ResultSet]
}
