package com.evolution.kafka.journal

import cats.Parallel
import cats.effect.*
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.evolution.kafka.journal.HeadCache.Eventual
import com.evolution.kafka.journal.conversions.ConsRecordToActionHeader
import com.evolution.kafka.journal.eventual.EventualJournal
import com.evolution.scache.Cache
import com.evolutiongaming.catshelper.*
import com.evolutiongaming.skafka.consumer.ConsumerConfig
import com.evolutiongaming.skafka.{Offset, Partition, Topic}

/**
 * [[HeadCache]] sharded between the pods of an application.
 *
 * Instead of every pod consuming every partition, as [[HeadCache#make]] does, the pods of an
 * application share a Kafka consumer group per topic. Each pod caches only the partitions assigned
 * to it, and asks the owner of any other partition through [[HeadCacheTransport]]. A request for a
 * partition owned by the same pod never goes through the transport.
 *
 * As in [[HeadCache#make]], partitions are consumed from the offset replicated to Cassandra, and
 * committed offsets are never used.
 *
 * Topics listed in [[ShardedHeadCacheConfig#eagerTopics]] start consuming when the cache is
 * created, the others on the first [[HeadCache#get]] for them. Topics never expire: a pod leaving a
 * consumer group would rebalance every pod in it.
 *
 * @see
 *   [[ShardedTopicCache]] for the details of consuming and rebalancing.
 */
object ShardedHeadCache {

  /**
   * Creates the cache using a Kafka configuration and Cassandra reader.
   *
   * @param kafkaConsumerOf
   *   Creates the Kafka consumers, one per topic.
   * @param consumerConfig
   *   Kafka consumer configuration. The consumer group, offset reset, commits and assignment
   *   strategy are overridden, see [[ShardedTopicCache.Consumer#make]].
   * @param eventualJournal
   *   Cassandra data source of the replicated offsets.
   * @param transport
   *   Communication with the other pods of the application.
   * @param config
   *   Application id and eager topics.
   * @param headCacheConfig
   *   Timeouts and size limits, as for [[HeadCache#make]]. [[HeadCacheConfig#expiry]] is not used.
   * @param metrics
   *   Metrics, the same as for [[HeadCache#make]]. The `result` label of `get` also takes the
   *   values `remote`, `remote_none`, `remote_failure` and `released`.
   */
  def make[F[_]: Async: Parallel: Runtime: LogOf: MeasureDuration: FromTry: FromJsResult: JsonCodec.Decode](
    kafkaConsumerOf: KafkaConsumerOf[F],
    consumerConfig: ConsumerConfig,
    eventualJournal: EventualJournal[F],
    transport: HeadCacheTransport[F],
    config: ShardedHeadCacheConfig,
    headCacheConfig: HeadCacheConfig,
    metrics: Option[HeadCacheMetrics[F]],
  ): Resource[F, HeadCache[F]] = {
    make(
      eventual = Eventual(eventualJournal),
      consumer = (groupId: String) => ShardedTopicCache.Consumer.make[F](kafkaConsumerOf, consumerConfig, groupId),
      transport = transport,
      config = config,
      headCacheConfig = headCacheConfig,
      metrics = metrics,
    )
  }

  // marked package-private to be called in unit tests with a fake consumer implementation, is not public API
  private[journal] def make[F[_]: Async: Parallel: Runtime: LogOf: MeasureDuration: FromJsResult: JsonCodec.Decode](
    eventual: Eventual[F],
    consumer: String => Resource[F, ShardedTopicCache.Consumer[F]],
    transport: HeadCacheTransport[F],
    config: ShardedHeadCacheConfig,
    headCacheConfig: HeadCacheConfig,
    metrics: Option[HeadCacheMetrics[F]],
  ): Resource[F, HeadCache[F]] = {
    for {
      log <- LogOf[F].apply(ShardedHeadCache.getClass).toResource
      cache <- Cache.loading[F, Topic, ShardedTopicCache[F]]
      cache <- metrics.fold(cache.pure[Resource[F, *]]) { metrics => cache.withMetrics(metrics.cache) }
      main = new Main(
        cache = cache,
        eventual = eventual,
        consumer = consumer,
        transport = transport,
        config = config,
        headCacheConfig = headCacheConfig,
        metrics = metrics.map { _.headCache },
        consRecordToActionHeader = ConsRecordToActionHeader[F],
        log = log,
      )
      _ <- main.init.toResource
      result <- main.withFence
    } yield {
      result.withLog(log)
    }
  }

  private final class Main[F[_]: Async: Parallel: MeasureDuration](
    cache: Cache[F, Topic, ShardedTopicCache[F]],
    eventual: Eventual[F],
    consumer: String => Resource[F, ShardedTopicCache.Consumer[F]],
    transport: HeadCacheTransport[F],
    config: ShardedHeadCacheConfig,
    headCacheConfig: HeadCacheConfig,
    metrics: Option[HeadCache.Metrics[F]],
    consRecordToActionHeader: ConsRecordToActionHeader[F],
    log: Log[F],
  ) extends HeadCache[F] {

    def get(key: Key, partition: Partition, offset: Offset): F[Option[HeadInfo]] = {
      topicCache(key.topic).flatMap { _.get(key.id, partition, offset) }
    }

    /**
     * Starts consuming the eager topics.
     */
    private[ShardedHeadCache] def init: F[Unit] = {
      config.eagerTopics.toVector.parTraverse_ { topic => topicCache(topic) }
    }

    private def topicCache(topic: Topic): F[ShardedTopicCache[F]] = {
      cache.getOrUpdateResource(topic) {
        val groupId = ShardedHeadCacheConfig.groupId(config.applicationId, topic)
        val log1 = log.withField("topic", topic)
        for {
          _ <- log1.info(s"starting, groupId=$groupId").toResource
          cache <- ShardedTopicCache.make(
            topic = topic,
            eventual = eventual,
            consumer = consumer(groupId),
            transport = transport,
            config = headCacheConfig,
            pointerTimeout = config.pointerTimeout,
            consRecordToActionHeader = consRecordToActionHeader,
            metrics = metrics,
            log = log1,
          )
        } yield cache
      }
    }
  }
}
