package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.implicits._
import com.datastax.driver.core.ConsistencyLevel
import com.evolutiongaming.config.ConfigHelper._
import com.evolutiongaming.scassandra.{CassandraConfig, QueryConfig}
import com.typesafe.config.Config
import pureconfig.error.{ConfigReaderFailures, ThrowableFailure}
import pureconfig.{ConfigCursor, ConfigReader}

import scala.util.Try


final case class EventualCassandraConfig(
  retries: Int = 100,
  segmentSize: Int = 100000,
  client: CassandraConfig = CassandraConfig(
    name = "journal",
    query = QueryConfig(
      consistency = ConsistencyLevel.LOCAL_QUORUM,
      fetchSize = 100,
      defaultIdempotence = true)),
  schema: SchemaConfig = SchemaConfig.default)

object EventualCassandraConfig {

  val default: EventualCassandraConfig = EventualCassandraConfig()


  implicit val configReaderEventualCassandraConfig: ConfigReader[EventualCassandraConfig] = {
    cursor: ConfigCursor => {
      for {
        cursor    <- cursor.asObjectCursor
        cassandra  = Try { apply1(cursor.value.toConfig, default) }
        cassandra <- cassandra.toEither.leftMap(a => ConfigReaderFailures(ThrowableFailure(a, cursor.location)))
      } yield cassandra
    }
  }


  @deprecated("use ConfigReader instead", "0.0.87")
  def apply(config: Config): EventualCassandraConfig = apply(config, default)

  @deprecated("use ConfigReader instead", "0.0.87")
  def apply(config: Config, default: => EventualCassandraConfig): EventualCassandraConfig = apply1(config, default)

  def apply1(config: Config, default: => EventualCassandraConfig): EventualCassandraConfig = {

    def get[T: FromConf](name: String) = config.getOpt[T](name)

    EventualCassandraConfig(
      retries = get[Int]("retries") getOrElse default.retries,
      segmentSize = get[Int]("segment-size") getOrElse default.segmentSize,
      client = get[Config]("client").fold(default.client)(CassandraConfig(_, default.client)),
      schema = get[Config]("schema").fold(default.schema)(SchemaConfig(_, default.schema)))
  }
}