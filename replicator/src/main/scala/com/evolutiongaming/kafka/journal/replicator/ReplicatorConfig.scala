package com.evolutiongaming.kafka.journal.replicator

import cats.data.{NonEmptyList => Nel}
import com.datastax.driver.core.ConsistencyLevel
import com.evolutiongaming.config.ConfigHelper._
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig
import com.evolutiongaming.scassandra.{CassandraConfig, QueryConfig}
import com.evolutiongaming.skafka.CommonConfig
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig}
import com.typesafe.config.Config

import scala.concurrent.duration._

final case class ReplicatorConfig(
  topicPrefixes: Nel[String] = Nel.of("journal"),
  topicDiscoveryInterval: FiniteDuration = 3.seconds,
  consumer: ConsumerConfig = ConsumerConfig(
    common = CommonConfig(
      clientId = Some("replicator"),
      receiveBufferBytes = 1000000),
    groupId = Some("replicator"),
    autoOffsetReset = AutoOffsetReset.Earliest,
    autoCommit = false),
  cassandra: EventualCassandraConfig = EventualCassandraConfig(
    client = CassandraConfig(
      name = "replicator",
      query = QueryConfig(
        consistency = ConsistencyLevel.LOCAL_QUORUM,
        defaultIdempotence = true))),
  pollTimeout: FiniteDuration = 10.millis)

object ReplicatorConfig {

  val Default: ReplicatorConfig = ReplicatorConfig()


  def apply(config: Config): ReplicatorConfig = apply(config, Default)

  def apply(config: Config, default: => ReplicatorConfig): ReplicatorConfig = {

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

    ReplicatorConfig(
      topicPrefixes = topicPrefixes,
      topicDiscoveryInterval = get[FiniteDuration]("topic-discovery-interval") getOrElse default.topicDiscoveryInterval,
      consumer = consumer,
      cassandra = get[Config]("cassandra").fold(default.cassandra)(EventualCassandraConfig.apply(_, default.cassandra)),
      pollTimeout = get[FiniteDuration]("kafka.consumer.poll-timeout") getOrElse default.pollTimeout)
  }
}