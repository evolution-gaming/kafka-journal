package com.evolutiongaming.kafka.journal

import com.evolutiongaming.skafka.consumer.ConsumerConfig
import com.evolutiongaming.skafka.producer.ProducerConfig
import com.typesafe.config.Config

final case class Settings(
  producer: ProducerConfig,
  consumer: ConsumerConfig)

object Settings {
  def apply(config: Config): Settings = ???
}
