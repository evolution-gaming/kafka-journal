package com.evolutiongaming.kafka.journal

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration._

final case class HeadCacheConfig(
  timeout: FiniteDuration = 3.seconds,
  cleanInterval: FiniteDuration = 100.millis,
  maxSize: Int = 100000)


object HeadCacheConfig {

  val default: HeadCacheConfig = HeadCacheConfig()

  implicit val configReaderHeadCacheConfig: ConfigReader[HeadCacheConfig] = deriveReader
}