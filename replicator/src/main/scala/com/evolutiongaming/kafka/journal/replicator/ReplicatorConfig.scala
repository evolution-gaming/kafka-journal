package com.evolutiongaming.kafka.journal.replicator

import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.datastax.driver.core.ConsistencyLevel
import com.evolutiongaming.config.ConfigHelper._
import com.evolutiongaming.kafka.journal.FromConfigReaderResult
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig
import com.evolutiongaming.kafka.journal.util.PureConfigHelper._
import com.evolutiongaming.scassandra.{CassandraConfig, QueryConfig}
import com.evolutiongaming.skafka.CommonConfig
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig}
import com.typesafe.config.Config
import pureconfig.{ConfigCursor, ConfigReader, ConfigSource}
import pureconfig.error.{ConfigReaderFailures, ThrowableFailure}

import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.Try

final case class ReplicatorConfig(
  topicPrefixes: Nel[String] = Nel.of("journal"),
  topicDiscoveryInterval: FiniteDuration = 3.seconds,
  consumer: ConsumerConfig = ConsumerConfig(
    common = CommonConfig(
      clientId = Some("replicator"),
      receiveBufferBytes = 1000000),
    groupId = Some("replicator"),
    autoOffsetReset = AutoOffsetReset.Earliest,
    autoCommit = false,
    maxPollRecords = 1000),
  cassandra: EventualCassandraConfig = EventualCassandraConfig(
    client = CassandraConfig(
      name = "replicator",
      query = QueryConfig(
        consistency = ConsistencyLevel.LOCAL_QUORUM,
        defaultIdempotence = true))),
  pollTimeout: FiniteDuration = 10.millis)

object ReplicatorConfig {

  val default: ReplicatorConfig = ReplicatorConfig()


  implicit val configReaderReplicatorConfig: ConfigReader[ReplicatorConfig] = {
    cursor: ConfigCursor => {
      for {
        cursor  <- cursor.asObjectCursor
        journal  = Try { apply1(cursor.value.toConfig, default) }
        journal <- journal.toEither.leftMap(a => ConfigReaderFailures(ThrowableFailure(a, cursor.location)))
      } yield journal
    }
  }


  def fromConfig[F[_]: FromConfigReaderResult](config: Config): F[ReplicatorConfig] = {
    ConfigSource
      .fromConfig(config)
      .at("evolutiongaming.kafka-journal.replicator")
      .load[ReplicatorConfig]
      .liftTo[F]
  }


  @deprecated("use ConfigReader instead", "0.0.87")
  def apply(config: Config): ReplicatorConfig = apply(config, default)

  @deprecated("use ConfigReader instead", "0.0.87")
  def apply(config: Config, default: => ReplicatorConfig): ReplicatorConfig = apply1(config, default)

  private def apply1(config: Config, default: => ReplicatorConfig): ReplicatorConfig = {
    def get[T: FromConf](name: String) = config.getOpt[T](name)

    val topicPrefixes = {
      val prefixes = for {
        prefixes <- get[List[String]]("topic-prefixes")
        prefixes <- Nel.fromList(prefixes)
      } yield prefixes
      prefixes getOrElse default.topicPrefixes
    }

    def consumer = {
      val config = for {
        kafka <- get[Config]("kafka")
        consumer <- kafka.getOpt[Config]("consumer")
      } yield {
        val config = consumer withFallback kafka
        ConsumerConfig(config, default.consumer)
      }
      config getOrElse default.consumer
    }

    val source = ConfigSource.fromConfig(config)

    def get1[A : ConfigReader : ClassTag](name: String) = {
      val source1 = source.at(name)
      source1.value().fold(_ => none[A], _ => source1.loadOrThrow[A].some)
    }

    ReplicatorConfig(
      topicPrefixes = topicPrefixes,
      topicDiscoveryInterval = get[FiniteDuration]("topic-discovery-interval") getOrElse default.topicDiscoveryInterval,
      consumer = consumer,
      cassandra = get1[EventualCassandraConfig]("cassandra") getOrElse default.cassandra,
      pollTimeout = get[FiniteDuration]("kafka.consumer.poll-timeout") getOrElse default.pollTimeout)
  }
}