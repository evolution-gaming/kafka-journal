package com.evolutiongaming.kafka.journal.snapshot.cassandra

import com.datastax.driver.core.ConsistencyLevel
import com.evolutiongaming.kafka.journal.cassandra.CassandraConsistencyConfig
import com.evolutiongaming.scassandra.{CassandraConfig, QueryConfig}
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

/** Cassandra-specific configuration used by a plugin.
  *
  * Specifies long time storage configuration and Cassandra client parameters.
  * 
  * Note: if `useLWT` is set to `true`, then `consistencyConfig.read` should be set to `ConsistencyLevel.SERIAL` or
  * `ConsistencyLevel.LOCAL_SERIAL`. Otherwise, the plugin will throw an exception to prevent data corruption.
  *
  * @param retries
  *   Number of retries in [[com.evolutiongaming.scassandra.NextHostRetryPolicy]]. It will retry doing a request on the
  *   same host if it timed out, or switch to another host if error happened, or the host was not available on a first
  *   attempt.
  * @param numberOfSnapshots
  *   Maximum number of snapshots to be stored per single persistence id. If the number of snapshots reaches this
  *   number, but a new snapshot is requsted to be written, then the oldest snapshot will be overwritten.
  * @param client
  *   Cassandra client configuration, see [[CassandraConfig]] for more details.
  * @param schema
  *   Schema of Cassandra database, i.e. keyspace, names of the tables etc. It also contains a flag if schema should be
  *   automatically created if not present, which is useful for integration testing purposes etc.
  * @param consistencyConfig
  *   Consistency levels to use for read and for write statements to Cassandra. The main reason one may be interested to
  *   change it, is for integration tests with small number of Cassandra nodes.
  * @param useLWT
  *   Use Cassandra LWTs to ensure the older snapshots of one writer do not overwrite the newer snapshots of another.
  *   It is recommended to set it to `false` and rely on external mechanism to ensure there is only a single writer
  *   (such as Akka Persistence).
  */
final case class SnapshotCassandraConfig(
  retries: Int = 100,
  numberOfSnapshots: Int = 10,
  client: CassandraConfig = CassandraConfig(
    name = "snapshot",
    query = QueryConfig(consistency = ConsistencyLevel.LOCAL_QUORUM, fetchSize = 1000, defaultIdempotence = true)
  ),
  schema: SnapshotSchemaConfig = SnapshotSchemaConfig.default,
  consistencyConfig: CassandraConsistencyConfig = CassandraConsistencyConfig.default,
  useLWT: Boolean = false
)

object SnapshotCassandraConfig {

  implicit val configReaderEventualCassandraConfig: ConfigReader[SnapshotCassandraConfig] = deriveReader

  val default: SnapshotCassandraConfig = SnapshotCassandraConfig()

}
