package com.evolutiongaming.kafka.journal.replicator

import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.datastax.driver.core.ConsistencyLevel
import com.evolutiongaming.kafka.journal.FromConfigReaderResult
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig
import com.evolutiongaming.kafka.journal.util.PureConfigHelper._
import com.evolutiongaming.scassandra.{CassandraConfig, QueryConfig}
import com.evolutiongaming.skafka.CommonConfig
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig}
import com.evolutiongaming.skafka.producer.ProducerConfig
import com.typesafe.config.Config
import pureconfig.{ConfigCursor, ConfigReader, ConfigSource}
import pureconfig.error.{ConfigReaderFailures, ThrowableFailure}

import scala.concurrent.duration._
import scala.util.Try

final case class ReplicatorConfig(
  topicPrefixes: Nel[String] = Nel.of("journal"),
  topicDiscoveryInterval: FiniteDuration = 3.seconds,
  cacheExpireAfter: FiniteDuration = 5.minutes,
  consumer: ConsumerConfig = ConsumerConfig(
    common = CommonConfig(
      clientId = "replicator".some,
      receiveBufferBytes = 1000000),
    groupId = "replicator".some,
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
        journal  = Try { fromConfig(cursor.value.toConfig, default) }
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


  private def fromConfig(config: Config, default: => ReplicatorConfig): ReplicatorConfig = {

    val source = ConfigSource.fromConfig(config)

    def get[A : ConfigReader](name: String) = source.at(name).load[A]

    val topicPrefixes = {
      val prefixes = for {
        prefixes <- get[List[String]]("topic-prefixes").toOption
        prefixes <- prefixes.toNel
      } yield prefixes
      prefixes getOrElse default.topicPrefixes
    }

    def kafka(name: String) = {
      get[Config]("kafka").map { kafka =>
        ConfigSource.fromConfig(kafka)
          .at(name)
          .load[Config]
          .fold(_ => kafka, _.withFallback(kafka))
      }
    }

    ReplicatorConfig(
      topicPrefixes = topicPrefixes,
      topicDiscoveryInterval = get[FiniteDuration]("topic-discovery-interval") getOrElse default.topicDiscoveryInterval,
//      producer = kafka("producer").fold(_ => default.producer, ProducerConfig(_, default.producer)),
      consumer = kafka("consumer").fold(_ => default.consumer, ConsumerConfig(_, default.consumer)),
      cassandra = get[EventualCassandraConfig]("cassandra") getOrElse default.cassandra,
      pollTimeout = get[FiniteDuration]("kafka.consumer.poll-timeout") getOrElse default.pollTimeout)
  }
}