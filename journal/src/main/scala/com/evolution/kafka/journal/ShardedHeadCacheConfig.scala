package com.evolution.kafka.journal

import com.evolutiongaming.skafka.Topic

import scala.concurrent.duration.*

/**
 * Configuration of [[ShardedHeadCache]], in addition to [[HeadCacheConfig]].
 *
 * @param applicationId
 *   Identifies the application whose pods share the cache. Pods of different applications have
 *   independent caches even for the same topic.
 * @param eagerTopics
 *   Topics to start consuming when the cache is created. Other topics start on the first
 *   [[HeadCache#get]] for them.
 * @param pointerTimeout
 *   How long to wait for the replicated offset of a partition, read from Cassandra when the
 *   partition is assigned. The read blocks the consumer, so it must be bounded. If it times out,
 *   the consumer fails and is created again.
 */
final case class ShardedHeadCacheConfig(
  applicationId: String,
  eagerTopics: Set[Topic] = Set.empty,
  pointerTimeout: FiniteDuration = 10.seconds,
)

object ShardedHeadCacheConfig {

  /**
   * Consumer group id of a topic.
   */
  def groupId(applicationId: String, topic: Topic): String = s"$applicationId-headcache-$topic"
}
