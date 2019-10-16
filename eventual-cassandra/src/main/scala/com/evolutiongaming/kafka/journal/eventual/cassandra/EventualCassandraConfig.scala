package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.datastax.driver.core.ConsistencyLevel
import com.evolutiongaming.scassandra.{CassandraConfig, QueryConfig}
import com.typesafe.config.Config
import pureconfig.generic.semiauto.deriveReader
import pureconfig.{ConfigReader, ConfigSource}


final case class EventualCassandraConfig(
  retries: Int = 100,
  segmentSize: SegmentSize = SegmentSize.default,
  client: CassandraConfig = CassandraConfig(
    name = "journal",
    query = QueryConfig(
      consistency = ConsistencyLevel.LOCAL_QUORUM,
      fetchSize = 1000,
      defaultIdempotence = true)),
  schema: SchemaConfig = SchemaConfig.default)

object EventualCassandraConfig {

  val default: EventualCassandraConfig = EventualCassandraConfig()


  implicit val configReaderEventualCassandraConfig: ConfigReader[EventualCassandraConfig] = deriveReader


  @deprecated("use ConfigReader instead", "0.0.87")
  def apply(config: Config): EventualCassandraConfig = ConfigSource.fromConfig(config).loadOrThrow[EventualCassandraConfig]
}