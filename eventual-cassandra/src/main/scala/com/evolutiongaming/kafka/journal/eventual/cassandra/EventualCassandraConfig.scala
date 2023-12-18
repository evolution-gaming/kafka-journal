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
  *   [[com.evolutiongaming.scassandra.NextHostRetryPolicy]]. It will retry
  *   doing a request on the same host if it timed out, or switch to another
  *   host if error happened, or the host was not available on a first attempt.
  * @param segmentSize
  *   Number of records per one segment. The larger the number, the better the
  *   chance the recovery will hit the same partition, and be faster, but too
  *   large numbers are also will make larger Cassandra partitions with all the
  *   associated issues. See [[SegmentSize]] for more details.
  * @param client
  *   Cassandra client configuration, see [[CassandraConfig]] for more details.
  * @param schema
  *   Schema of Cassandra database, i.e. keyspace, names of the tables etc. It
  *   also contains a flag if schema should be automatically created if not
  *   present, which is useful for integration testing purposes etc.
  * @param consistencyConfig
  *   Consistency levels to use for read and for write statements to Cassandra.
  *   The main reason one may be interested to change it, is for integration
  *   tests with small number of Cassandra nodes.
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
  consistencyConfig: CassandraConsistencyConfig = CassandraConsistencyConfig.default)

object EventualCassandraConfig {

  val default: EventualCassandraConfig = EventualCassandraConfig()

  implicit val configReaderEventualCassandraConfig: ConfigReader[EventualCassandraConfig] = deriveReader


  /** @deprecated Use [[com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraConsistencyConfig]] instead */
  @deprecated(since = "3.2.2", message = "The class was extracted to a separate class")
  type ConsistencyConfig = Nothing

}
