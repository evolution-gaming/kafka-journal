package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.datastax.driver.core.ConsistencyLevel
import com.evolutiongaming.scassandra.{CassandraConfig, QueryConfig}
import pureconfig.generic.semiauto.deriveReader
import pureconfig.ConfigReader

/** Cassandra-specific configuration used by a plugin.
  *
  * Specifies long time storage configuration and Cassandra client parameters.
  *
  * @param retries
  *   Number of retries in
  *   [[com.evolutiongaming.scassandra.NextHostRetryPolicy]].
  * @param segmentSize
  *   Number of records per one segment. See [[SegmentSize]] for more details.
  * @param client
  *   Cassandra client configuration, see [[CassandraConfig]] for more details.
  * @param schema
  *   Schema of Cassandra database, i.e. keyspace, names of the tables etc.
  * @param consistencyConfig
  *   Consistency levels to use for read and write statements to Cassandra.
  */
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
  consistencyConfig: EventualCassandraConfig.ConsistencyConfig = EventualCassandraConfig.ConsistencyConfig.default)

object EventualCassandraConfig {

  val default: EventualCassandraConfig = EventualCassandraConfig()

  implicit val configReaderEventualCassandraConfig: ConfigReader[EventualCassandraConfig] = deriveReader


  final case class ConsistencyConfig(
    read: ConsistencyConfig.Read = ConsistencyConfig.Read.default,
    write: ConsistencyConfig.Write = ConsistencyConfig.Write.default)

  object ConsistencyConfig {

    implicit val configReaderConsistencyConfig: ConfigReader[ConsistencyConfig] = deriveReader

    val default: ConsistencyConfig = ConsistencyConfig()


    final case class Read(value: ConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM)

    object Read {
      val default: Read = Read()

      implicit val configReaderRead: ConfigReader[Read] = ConfigReader[ConsistencyLevel].map { a => Read(a) }
    }


    final case class Write(value: ConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM)

    object Write {
      val default: Write = Write()

      implicit val configReaderWrite: ConfigReader[Write] = ConfigReader[ConsistencyLevel].map { a => Write(a) }
    }
  }
}
