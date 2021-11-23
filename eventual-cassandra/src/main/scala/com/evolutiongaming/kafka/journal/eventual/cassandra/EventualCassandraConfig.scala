package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.datastax.driver.core.ConsistencyLevel
import com.evolutiongaming.scassandra.{CassandraConfig, QueryConfig}
import pureconfig.generic.semiauto.deriveReader
import pureconfig.ConfigReader
import EventualCassandraConfig._


final case class EventualCassandraConfig(
  retries: Int = 100,
  segmentSize: SegmentSize = SegmentSize.default,
  client: CassandraConfig = CassandraConfig(
    name = "journal",
    query = QueryConfig(
      consistency = ConsistencyLevel.LOCAL_QUORUM,
      fetchSize = 1000,
      defaultIdempotence = true)),
  schema: SchemaConfig = SchemaConfig.default,
  consistencyConfig: ConsistencyConfig = ConsistencyConfig.default)

object EventualCassandraConfig {

  final case class ConsistencyConfig(
    read: ConsistencyConfig.Read = ConsistencyConfig.Read(ConsistencyLevel.LOCAL_QUORUM),
    write: ConsistencyConfig.Write = ConsistencyConfig.Write(ConsistencyLevel.LOCAL_QUORUM))

  object ConsistencyConfig {
    val default = ConsistencyConfig()

    final case class Read(value: ConsistencyLevel)
    final case class Write(value: ConsistencyLevel)
  }

  val default: EventualCassandraConfig = EventualCassandraConfig()

  implicit val configReaderRead: ConfigReader[ConsistencyConfig.Read] = deriveReader
  implicit val configReaderWrite: ConfigReader[ConsistencyConfig.Write] = deriveReader
  implicit val configReaderConsistencyCOnfig: ConfigReader[ConsistencyConfig] = deriveReader

  implicit val configReaderEventualCassandraConfig: ConfigReader[EventualCassandraConfig] = deriveReader
}