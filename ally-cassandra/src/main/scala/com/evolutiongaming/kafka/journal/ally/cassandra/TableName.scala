package com.evolutiongaming.kafka.journal.ally.cassandra

case class TableName(keyspace: String, table: String) {
  def asCql: String = s"$keyspace.$table"
}
