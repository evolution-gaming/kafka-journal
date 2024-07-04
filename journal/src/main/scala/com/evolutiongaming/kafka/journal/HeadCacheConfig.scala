package com.evolutiongaming.kafka.journal

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration._

/** Configuration of [[HeadCache]].
  *
  * @param timeout
  *   Duration to wait until [[HeadInfo]] appears in cache from either Kafka
  *   consumer or Cassandra poller before returning `None` in [[HeadCache#get]].
  *   See [[PartitionCache#of]] and [[PartitionCache.Result.Now.Timeout]] for
  *   more details on internals.
  * @param expiry
  *   Duration to keep [[TopicCache]] alive after a last read. Having such an
  *   expiration period decreases the load on the underlying databases for
  *   topics, which do not require to do recoveries lately.
  * @param removeInterval
  *   How often Cassandra is being polled for the information about already
  *   replicated records.
  * @param partition
  *   Partition cache configuration as described in [[HeadCacheConfig.Partition]].
  */
final case class HeadCacheConfig(
  timeout: FiniteDuration = 1.second,
  expiry: FiniteDuration = 10.minutes,
  removeInterval: FiniteDuration = 100.millis,
  partition: HeadCacheConfig.Partition = HeadCacheConfig.Partition.default)


object HeadCacheConfig {

  /** Recommended configuration of [[HeadCache]] */
  val default: HeadCacheConfig = HeadCacheConfig()

  implicit val configReaderHeadCacheConfig: ConfigReader[HeadCacheConfig] = deriveReader

  /** Partition cache configuration.
    *
    * @param maxSize
    *   Maximum number of journals to store for a single partition, i.e. in
    *   [[PartitionCache]]. I.e. the total maximum number of journals store in
    *   [[TopicCache]] will be equal to `[number of partitions in topic] x
    *   [maxSize]`.
    * @param dropUponLimit
    *   Proportion of number of journals to drop from a partition cache if
    *   `maxSize` is reached. Value outside of the range of `0.01` to `1.0` will
    *   be ignored. `0.01` means that 1% of journals will get dropped, and `1.0`
    *   means that 100% of journals will get dropped.
    *
    * @see
    *   [[PartitionCache#of]] for more details.
    */
  final case class Partition(maxSize: Int = 10000, dropUponLimit: Double = 0.1)

  object Partition {

    /** Recommended configuration of partition cache */
    val default: Partition = Partition()

    implicit val configReaderPartition: ConfigReader[Partition] = deriveReader
  }
}
