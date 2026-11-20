package com.evolution.kafka.journal.eventual.cassandra

import com.evolution.kafka.journal.cassandra.CassandraConsistencyConfig
import com.evolution.scassandra.CassandraConfig
import com.typesafe.config.ConfigFactory
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

/**
 * Cassandra-specific configuration used by a plugin.
 *
 * Specifies long time storage configuration and Cassandra client parameters.
 *
 * @param retries
 *   Number of retries in [[com.evolution.scassandra.NextHostRetryPolicy]]. It will retry doing a
 *   request on the same host if it timed out, or switch to another host if error happened, or the
 *   host was not available on a first attempt.
 * @param segmentSize
 *   Number of records per one segment. The larger the number, the better the chance the recovery
 *   will hit the same partition, and be faster, but too large numbers are also will make larger
 *   Cassandra partitions with all the associated issues. See [[SegmentSize]] for more details.
 * @param client
 *   Cassandra client configuration, i.e. the configuration of the Java driver 4 in the format of
 *   its `datastax-java-driver` section, see [[CassandraConfig]] for more details. The defaults of
 *   the driver are used for the missing options, but not [[EventualCassandraConfig.DefaultClient]],
 *   which is used only if `client` is not configured at all.
 * @param schema
 *   Schema of Cassandra database, i.e. keyspace, names of the tables etc. It also contains a flag
 *   if schema should be automatically created if not present, which is useful for integration
 *   testing purposes etc.
 * @param consistencyConfig
 *   Consistency levels to use for read and for write statements to Cassandra. The main reason one
 *   may be interested to change it, is for integration tests with small number of Cassandra nodes.
 */
final case class EventualCassandraConfig(
  retries: Int = 100,
  segmentSize: SegmentSize = SegmentSize.default,
  client: CassandraConfig = EventualCassandraConfig.DefaultClient,
  schema: SchemaConfig = SchemaConfig.default,
  consistencyConfig: CassandraConsistencyConfig = CassandraConsistencyConfig.default,
)

object EventualCassandraConfig {

  /**
   * Equivalent of the defaults used with the Java driver 3: `LOCAL_QUORUM` consistency, page size
   * of 1000 rows and idempotent statements
   */
  lazy val DefaultClient: CassandraConfig = CassandraConfig(
    ConfigFactory.parseString(
      """
        |basic.session-name = "journal"
        |basic.request.consistency = LOCAL_QUORUM
        |basic.request.page-size = 1000
        |basic.request.default-idempotence = true
        |""".stripMargin,
    ),
  )

  val default: EventualCassandraConfig = EventualCassandraConfig()

  implicit val configReaderEventualCassandraConfig: ConfigReader[EventualCassandraConfig] = deriveReader
}
