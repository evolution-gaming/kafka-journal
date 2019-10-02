package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.implicits._
import com.datastax.driver.core.ConsistencyLevel
import com.evolutiongaming.scassandra.{CassandraConfig, QueryConfig}
import com.typesafe.config.Config
import pureconfig.error.{ConfigReaderFailures, ThrowableFailure}
import pureconfig.generic.semiauto.deriveReader
import pureconfig.{ConfigCursor, ConfigReader, ConfigSource}

import scala.util.Try


final case class EventualCassandraConfig(
  retries: Int = 100,
  segmentSize: SegmentSize = SegmentSize.default,
  client: CassandraConfig = CassandraConfig(
    name = "journal",
    query = QueryConfig(
      consistency = ConsistencyLevel.LOCAL_QUORUM,
      fetchSize = 100,
      defaultIdempotence = true)),
  schema: SchemaConfig = SchemaConfig.default)

object EventualCassandraConfig {

  val default: EventualCassandraConfig = EventualCassandraConfig()


  // TODO move to scassandra
  private implicit val configReaderCassandraConfig: ConfigReader[CassandraConfig] = {
    cursor: ConfigCursor => {
      for {
        cursor    <- cursor.asObjectCursor
        cassandra  = Try { CassandraConfig(cursor.value.toConfig) }
        cassandra <- cassandra.toEither.leftMap(a => ConfigReaderFailures(ThrowableFailure(a, cursor.location)))
      } yield cassandra
    }
  }


  implicit val configReaderEventualCassandraConfig: ConfigReader[EventualCassandraConfig] = deriveReader


  @deprecated("use ConfigReader instead", "0.0.87")
  def apply(config: Config): EventualCassandraConfig = ConfigSource.fromConfig(config).loadOrThrow[EventualCassandraConfig]
}