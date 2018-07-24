package com.evolutiongaming.kafka.journal.replicator

import com.evolutiongaming.config.ConfigHelper._
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.consumer.ConsumerConfig
import com.typesafe.config.Config

import scala.concurrent.duration._

case class ReplicatorConfig(
  topicPrefixes: Nel[String] = Nel("journal"),
  topicDiscoveryInterval: FiniteDuration = 3.seconds,
  consumerConfig: ConsumerConfig = ConsumerConfig.Default)

object ReplicatorConfig {

  val Default: ReplicatorConfig = ReplicatorConfig()


  def apply(config: Config): ReplicatorConfig = {

    def get[T: FromConf](name: String) = config.getOpt[T](name)

    val topicPrefixes = {
      val prefixes = for {
        prefixes <- get[List[String]]("topic-prefixes")
        prefixes <- Nel.opt(prefixes)
      } yield prefixes
      prefixes getOrElse Default.topicPrefixes
    }

    ReplicatorConfig(
      topicPrefixes = topicPrefixes,
      topicDiscoveryInterval = get[FiniteDuration]("topic-discovery-interval") getOrElse Default.topicDiscoveryInterval,
      consumerConfig = get[Config]("kafka.consumer").fold(Default.consumerConfig)(ConsumerConfig.apply))
  }
}