package com.evolutiongaming.kafka.journal

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration._

final case class HeadCacheConfig(
  timeout: FiniteDuration = 1.second,
  expiry: FiniteDuration = 10.minutes,
  removeInterval: FiniteDuration = 100.millis,
  partition: HeadCacheConfig.Partition = HeadCacheConfig.Partition.default,
  reloadPartitionSetInterval: FiniteDuration = 5.minutes)


object HeadCacheConfig {

  val default: HeadCacheConfig = HeadCacheConfig()

  implicit val configReaderHeadCacheConfig: ConfigReader[HeadCacheConfig] = deriveReader

  final case class Partition(maxSize: Int = 10000, dropUponLimit: Double = 0.1)

  object Partition {
    val default: Partition = Partition()

    implicit val configReaderPartition: ConfigReader[Partition] = deriveReader
  }
}