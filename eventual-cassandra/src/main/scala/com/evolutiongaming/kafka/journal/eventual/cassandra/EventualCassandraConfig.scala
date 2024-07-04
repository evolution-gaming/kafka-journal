package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.datastax.driver.core.ConsistencyLevel
import com.evolutiongaming.kafka.journal.cassandra.CassandraConsistencyConfig
import com.evolutiongaming.scassandra.{CassandraConfig, QueryConfig}
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import EventualCassandraConfig._

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
  consistencyConfig: ConsistencyConfig = ConsistencyConfig.default)

object EventualCassandraConfig {

  val default: EventualCassandraConfig = EventualCassandraConfig()

  implicit val configReaderEventualCassandraConfig: ConfigReader[EventualCassandraConfig] = deriveReader


  @deprecated(since = "3.3.9", message = "Use [[CassandraConsistencyConfig]] instead")
  final case class ConsistencyConfig( 
    read: ConsistencyConfig.Read = ConsistencyConfig.Read.default,
    write: ConsistencyConfig.Write = ConsistencyConfig.Write.default) {

    private[cassandra] def toCassandraConsistencyConfig: CassandraConsistencyConfig =
      CassandraConsistencyConfig(
        read = this.read.toCassandraConsistencyConfig,
        write = this.write.toCassandraConsistencyConfig,
      )
    
  }

  @deprecated(since = "3.3.9", message = "Use [[CassandraConsistencyConfig]] instead")
  object ConsistencyConfig {

    implicit val configReaderConsistencyConfig: ConfigReader[ConsistencyConfig] = deriveReader

    val default: ConsistencyConfig = ConsistencyConfig()

    final case class Read(value: ConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM) {
      private[cassandra] def toCassandraConsistencyConfig: CassandraConsistencyConfig.Read =
        CassandraConsistencyConfig.Read(this.value)
    }

    object Read {
      val default: Read = Read()

      implicit val configReaderRead: ConfigReader[Read] = ConfigReader[ConsistencyLevel].map { a => Read(a) }
    }


    final case class Write(value: ConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM) {
      private[cassandra] def toCassandraConsistencyConfig: CassandraConsistencyConfig.Write =
        CassandraConsistencyConfig.Write(this.value)
    }

    object Write {

      val default: Write = Write()

      implicit val configReaderWrite: ConfigReader[Write] = ConfigReader[ConsistencyLevel].map { a => Write(a) }
    }
  }

}
