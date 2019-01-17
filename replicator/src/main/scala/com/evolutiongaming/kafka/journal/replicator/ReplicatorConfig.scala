package com.evolutiongaming.kafka.journal.replicator

import com.evolutiongaming.config.ConfigHelper._
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.consumer.ConsumerConfig
import com.typesafe.config.Config

import scala.concurrent.duration._

final case class ReplicatorConfig(
  topicPrefixes: Nel[String] = Nel("journal"),
  topicDiscoveryInterval: FiniteDuration = 3.seconds,
  consumer: ConsumerConfig = ConsumerConfig.Default,
  cassandra: EventualCassandraConfig = EventualCassandraConfig.Default,
  pollTimeout: FiniteDuration = 50.millis)

object ReplicatorConfig {

  val Default: ReplicatorConfig = ReplicatorConfig()

  def apply(config: Config): ReplicatorConfig = {
    apply(config, Default)
  }

  def apply(config: Config, default: => ReplicatorConfig): ReplicatorConfig = {

    def get[T: FromConf](name: String) = config.getOpt[T](name)

    val topicPrefixes = {
      val prefixes = for {
        prefixes <- get[List[String]]("topic-prefixes")
        prefixes <- Nel.opt(prefixes)
      } yield prefixes
      prefixes getOrElse default.topicPrefixes
    }

    def consumer = {
      val config = for {
        kafka <- get[Config]("kafka")
        consumer <- kafka.getOpt[Config]("consumer")
      } yield {
        val config = consumer withFallback kafka
        ConsumerConfig(config)
      }
      config getOrElse default.consumer
    }

    ReplicatorConfig(
      topicPrefixes = topicPrefixes,
      topicDiscoveryInterval = get[FiniteDuration]("topic-discovery-interval") getOrElse default.topicDiscoveryInterval,
      consumer = consumer,
      cassandra = get[Config]("cassandra").fold(default.cassandra)(EventualCassandraConfig.apply),
      pollTimeout = get[FiniteDuration]("kafka.consumer.poll-timeout") getOrElse default.pollTimeout)
  }
}