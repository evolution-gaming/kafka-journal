package com.evolutiongaming.kafka.journal

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration._


final case class JournalConfig(
  pollTimeout: FiniteDuration = 10.millis,
  kafka: KafkaConfig = KafkaConfig("journal"),
  headCache: JournalConfig.HeadCache = JournalConfig.HeadCache.default)

object JournalConfig {

  val default: JournalConfig = JournalConfig()

  implicit val configReaderJournalConfig: ConfigReader[JournalConfig] = {
    implicit val configReaderKafkaConfig = KafkaConfig.configReader(default.kafka)
    deriveReader[JournalConfig]
  }


  final case class HeadCache(enabled: Boolean = true)

  object HeadCache {

    val default: HeadCache = HeadCache()

    implicit val configReaderHeadCache: ConfigReader[HeadCache] = deriveReader
  }
}