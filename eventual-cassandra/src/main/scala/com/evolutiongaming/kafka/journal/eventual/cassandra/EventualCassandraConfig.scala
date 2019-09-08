package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.datastax.driver.core.ConsistencyLevel
import com.evolutiongaming.scassandra.{CassandraConfig, QueryConfig}
import com.evolutiongaming.config.ConfigHelper._
import com.typesafe.config.Config

final case class EventualCassandraConfig(
  retries: Int = 100,
  segmentSize: Int = 100000,
  client: CassandraConfig = CassandraConfig(
    name = "journal",
    query = QueryConfig(
      consistency = ConsistencyLevel.LOCAL_QUORUM,
      fetchSize = 100,
      defaultIdempotence = true)),
  schema: SchemaConfig = SchemaConfig.Default)

object EventualCassandraConfig {

  val Default: EventualCassandraConfig = EventualCassandraConfig()


  // TODO pure config
  def apply(config: Config): EventualCassandraConfig = apply(config, Default)

  def apply(config: Config, default: => EventualCassandraConfig): EventualCassandraConfig = {

    def get[T: FromConf](name: String) = config.getOpt[T](name)

    EventualCassandraConfig(
      retries = get[Int]("retries") getOrElse default.retries,
      segmentSize = get[Int]("segment-size") getOrElse default.segmentSize,
      client = get[Config]("client").fold(default.client)(CassandraConfig(_, default.client)),
      schema = get[Config]("schema").fold(default.schema)(SchemaConfig(_, default.schema)))
  }
}