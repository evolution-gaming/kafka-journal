package com.evolutiongaming.cassandra

import com.typesafe.config.Config

case class CassandraConfig()

object CassandraConfig {
  def apply(config: Config): CassandraConfig = ???
}
