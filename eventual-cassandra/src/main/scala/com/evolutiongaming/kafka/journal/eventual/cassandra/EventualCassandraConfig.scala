package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.cassandra.CassandraConfig
import com.evolutiongaming.config.ConfigHelper._
import com.typesafe.config.Config

final case class EventualCassandraConfig(
  segmentSize: Int = 1000000,
  client: CassandraConfig = CassandraConfig.Default,
  schema: SchemaConfig = SchemaConfig.Default)

object EventualCassandraConfig {

  val Default: EventualCassandraConfig = EventualCassandraConfig()


  def apply(config: Config): EventualCassandraConfig = {

    def get[T: FromConf](name: String) = config.getOpt[T](name)

    EventualCassandraConfig(
      segmentSize = get[Int]("segment-size") getOrElse Default.segmentSize,
      client = get[Config]("client").fold(Default.client)(CassandraConfig.apply),
      schema = get[Config]("schema").fold(Default.schema)(SchemaConfig.apply))
  }
}