package com.evolutiongaming.kafka.journal.eventual.cassandra

case class TableName(keyspace: String, table: String) {
  def asCql: String = s"$keyspace.$table"
}
