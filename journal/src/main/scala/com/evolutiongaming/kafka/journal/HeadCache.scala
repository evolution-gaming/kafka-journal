package com.evolutiongaming.kafka.journal

import cats._
import cats.effect._
import cats.effect.Async
import cats.effect.syntax.all._
import cats.syntax.all._
import com.evolution.scache.{Cache, ExpiringCache}
import com.evolutiongaming.catshelper._
import com.evolutiongaming.kafka.journal.PartitionCache.Result
import com.evolutiongaming.kafka.journal.conversions.ConsRecordToActionHeader
import com.evolutiongaming.kafka.journal.eventual.{EventualJournal, TopicPointers}
import com.evolutiongaming.skafka.consumer.ConsumerConfig
import com.evolutiongaming.skafka.{Offset, Partition, Topic}
import com.evolutiongaming.smetrics.MetricsHelper._
import com.evolutiongaming.smetrics._

import scala.concurrent.duration._

/** Metainfo of events written to Kafka, but not yet replicated to Cassandra.
  *
  * The implementation subscribes to all events in Kafka and periodically polls
  * Cassandra to remove information about the events, which already replicated.
  *
  * The returned entries do not contain the events themselves, but only an
  * offset of the first non-replicated event, the sequence number of last event,
  * range of events to be deleted etc.
  *
  * The consuming/polling of the records will stop after a configured timeout
  * (i.e. [[HeadCacheConfig#expiry]]) for the topics where no activity /
  * recoveries happen. It will restart when the new calls to a cache come for
  * these topics again.
  *
  * TODO headcache:
  * 1. Keep 1000 last seen entries, even if replicated.
  * 2. Fail headcache when background tasks failed
  *
  * @see [[HeadInfo]] for more details on the purpose of the stored data.
  */
trait HeadCache[F[_]] {

  /** Get the information about a state of a journal stored in the cache.
    *
    * @param key
    *   Journal key including a Kafka topic where journal is stored and
    *   a journal identifier.
    * @param partition
    *   Partition where journal is stored to. The usual way to get the partition
    *   is to write a "marker" record to Kafka topic and use the partition of
    *   the marker as a current one.
    * @param offset
    *   Current [[Offset]], i.e. maximum offset where Kafka records related to a
    *   journal are located. The usual way to get such an offset is to write a
    *   "marker" record to Kafka patition and use the offset of the marker as a
    *   current one.
    *
    * @return
    *   [[HeadInfo]] with the current metainformation about non-replicated
    *   events, or `None` if it was not present in [[HeadCache]] and could not
    *   be loaded either.
    */
  def get(key: Key, partition: Partition, offset: Offset): F[Option[HeadInfo]]
}

object HeadCache {

  /** Disable cache and always return `None` */
  def empty[F[_]: Applicative]: HeadCache[F] = const(none[HeadInfo].pure[F])

  /** Disable cache and always return a predefined value */
  def const[F[_]](value: F[Option[HeadInfo]]): HeadCache[F] = {
    class Const
    new Const with HeadCache[F] {
      def get(key: Key, partition: Partition, offset: Offset) = value
    }
  }

  /** Creates new cache using a Kafka configuration and Cassandra reader.
    *
    * The created instances will report metrics to `metrics` and also will do
    * the debug logging. There is no need to call [[HeadCache#withLogs]] on
    * them.
    *
    * @param consumerConfig
    *   Kafka consumer configuration used to find new non-replicated journal
    *   events. Some of the parameters will be ignored. See
    *   [[TopicCache.Consumer#of]] for more details.
    * @param eventualJournal
    *   Cassandra (or other long term storage) data source used to remove
    *   replicated events from the cache. Usually created by calling
    *   [[EventualCassandra#of]].
    * @param metrics
    *   Interface to report the metrics to. The intended way to configure it is
    *   overriding [[KafkaJournal#metrics]] in a custom implementation of
    *   [[KafkaJournal]].
    * @return
    *   Resource which will configure a [[HeadCache]] with the passed
    *   parameters. Instance of `Resource[HeadCache]` are, obviously, reusable
    *   and there is no need to call [[HeadCache#of]] each time if parameters
    *   did not change.
    */
  def of[F[_]: Async: Parallel: Runtime: LogOf: KafkaConsumerOf: MeasureDuration: FromTry: FromJsResult: JsonCodec.Decode](
    consumerConfig: ConsumerConfig,
    eventualJournal: EventualJournal[F],
    metrics: Option[HeadCacheMetrics[F]],
  ): Resource[F, HeadCache[F]] = {
    for {
      log    <- LogOf[F].apply(HeadCache.getClass).toResource
      result <- HeadCache.of(Eventual(eventualJournal), log, TopicCache.Consumer.of[F](consumerConfig), metrics)
      result <- result.withFence
    } yield {
      result.withLog(log)
    }
  }

  /** Creates new cache using Kafka and Cassandra data sources.
    *
    * The method also allows to change the default configuration in form of
    * [[HeadCacheConfig]], i.e. to make the polling faster for testing purposes.
    *
    * @param eventual
    *   Cassandra data source.
    * @param log
    *   Logger to use for [[TopicCache#withLog]]. Note, that only [[TopicCache]]
    *   debug logging will be affected by this. One needs to call
    *   [[HeadCache#withLog]] if debug logging for [[HeadCache]] is required.
    * @param consumer
    *   Kakfa data source factory. The reason why it is factory (i.e.
    *   `Resource`) is that [[HeadCache]] will try to recreate consumer in case
    *   of the failure.
    * @param metrics
    *   Interface to report the metrics to. The intended way to configure it is
    *   overriding [[KafkaJournal#metrics]] in a custom implementation of
    *   [[KafkaJournal]].
    * @param config
    *   Cache configuration. It is recommended to keep it default, and only
    *   change it for unit testing purposes.
    * @return
    *   Resource which will configure a [[HeadCache]] with the passed
    *   parameters. Instance of `Resource[HeadCache]` are, obviously, reusable
    *   and there is no need to call [[HeadCache#of]] each time if parameters
    *   did not change.
    */
  def of[F[_]: Async: Parallel: Runtime: FromJsResult: MeasureDuration: JsonCodec.Decode](
    eventual: Eventual[F],
    log: Log[F],
    consumer: Resource[F, TopicCache.Consumer[F]],
    metrics: Option[HeadCacheMetrics[F]],
    config: HeadCacheConfig = HeadCacheConfig.default,
  ): Resource[F, HeadCache[F]] = {

    val consRecordToActionHeader = ConsRecordToActionHeader[F]
    for {
      cache <- Cache.expiring(ExpiringCache.Config[F, Topic, TopicCache[F]](expireAfterRead = config.expiry), partitions = 1.some)
      cache <- metrics.fold(cache.pure[Resource[F, *]]) { metrics => cache.withMetrics(metrics.cache) }
    } yield {
      class Main
      new Main with HeadCache[F] {

        def get(key: Key, partition: Partition, offset: Offset) = {
          val topic = key.topic
          val log1  = log.prefixed(topic)
          cache
            .getOrUpdateResource(topic) {
              TopicCache
                .of(eventual, topic, log1, consumer, config, consRecordToActionHeader, metrics.map { _.headCache })
                .map { cache =>
                  metrics
                    .fold(cache) { metrics => cache.withMetrics(topic, metrics.headCache) }
                    .withLog(log1)
                }
            }
            .flatMap { cache =>
              cache
                .get(key.id, partition, offset)
                .flatMap { _.toNow }
                .map {
                  case a: Result.Now.Value   => a.value.some
                  case Result.Now.Ahead      => HeadInfo.empty.some
                  case Result.Now.Limited    => none
                  case _: Result.Now.Timeout => none
                }
            }
        }
      }
    }
  }

  /** Lighweight wrapper over [[EventualJournal]].
    *
    * Allows easier stubbing in unit tests.
    */
  trait Eventual[F[_]] {

    /** Gets the last replicated offset for a partition topic.
      *
      * @see [[EventualJournal#offset]] for more details.
      */
    def pointer(topic: Topic, partition: Partition): F[Option[Offset]]

  }

  object Eventual {

    def apply[F[_]](implicit F: Eventual[F]): Eventual[F] = F

    /** Wrap [[EventualJournal]] into [[Eventual]] */
    def apply[F[_]](eventualJournal: EventualJournal[F]): Eventual[F] = {
      class Main
      new Main with HeadCache.Eventual[F] {
        def pointer(topic: Topic, partition: Partition): F[Option[Offset]] = eventualJournal.offset(topic, partition)
      }
    }

    /** Always return `None` as an offset, i.e. pretend nothing ever replicates.
      *
      * Only useful for testing purposes.
      */
    def empty[F[_]: Applicative]: Eventual[F] = const(TopicPointers.empty.pure[F])

    /** Ignore topic and specify offset to return by partition.
      *
      * Only useful for testing purposes.
      */
    def const[F[_]: Applicative](value: F[TopicPointers]): Eventual[F] = {
      class Const
      new Const with Eventual[F] {
        def pointer(topic: Topic, partition: Partition): F[Option[Offset]] = value.map(_.values.get(partition))
      }
    }
  }

  private abstract sealed class WithFence

  private abstract sealed class WithLog

  implicit class HeadCacheOps[F[_]](val self: HeadCache[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): HeadCache[G] = new HeadCache[G] {

      def get(key: Key, partition: Partition, offset: Offset) = {
        f(self.get(key, partition, offset))
      }
    }

    /** Log debug messages on every call to the class methods.
      *
      * The messages will go to DEBUG level, so it is also necessary to enable
      * it in logger configuration.
      */
    def withLog(log: Log[F])(implicit F: FlatMap[F], measureDuration: MeasureDuration[F]): HeadCache[F] = {
      new WithLog with HeadCache[F] {

        def get(key: Key, partition: Partition, offset: Offset) = {
          for {
            d <- MeasureDuration[F].start
            a <- self.get(key, partition, offset)
            d <- d
            _ <- log.debug(s"get in ${d.toMillis}ms, key: $key, offset: $partition:$offset, result: $a")
          } yield a
        }
      }
    }

    /** Prevents cache methods to be used after `Resource` was released.
      *
      * [[ReleasedError]] will be raised if [[HeadCache#get]] is called after
      * resource is released.
      *
      * It may prevent certain kind of bugs, when several caches are,
      * accidentially, alive and working.
      */
    def withFence(implicit F: Sync[F]): Resource[F, HeadCache[F]] = {
      Resource
        .make { Ref[F].of(().pure[F]) } { _.set(ReleasedError.raiseError[F, Unit]) }
        .map { ref =>
          new WithFence with HeadCache[F] {
            def get(key: Key, partition: Partition, offset: Offset) = {
              ref
                .get
                .flatten
                .productR { self.get(key, partition, offset) }
            }
          }
        }
    }
  }

  /** Provides methods to update the metrics for [[HeadCache]] internals */
  trait Metrics[F[_]] {

    /** Report duration and result of cache hits, i.e. [[TopicCache#get]].
      *
      * @param topic
      *   Topic journal is being stored in.
      * @param latency
      *   Duration of [[TopicCache#get]] call.
      * @param result
      *   Result of the call, i.e. "ahead", "limited", "timeout" or "failure".
      * @param now
      *   If result was [[PartitionCache.Result.Now]], i.e. entry was already in
      *   cache.
      */
    def get(topic: Topic, latency: FiniteDuration, result: String, now: Boolean): F[Unit]

    /** Report health of all [[PartitionCache]] instances related to a topic.
      *
      * @param topic
      *   Topic which these [[PartitionCache]] instances are related to.
      * @param entries
      *   Number of distinct journals stored in a topic cache. If it is too
      *   close to [[HeadCacheConfig.Partition#maxSize]] multiplied by number of
      *   partitions, the cache might not work efficiently.
      * @param listeners
      *   Number of listeners waiting after [[PartitionCache#get]] call. Too
      *   many of them might mean that cache is not being loaded fast enough.
      */
    def meters(topic: Topic, entries: Int, listeners: Int): F[Unit]

    /** Report the latency and number of records coming from Kafka.
      *
      * I.e. how long it took for a next element in a stream returned by
      * [[HeadCacheConsumption#apply]] to get from a journal writer to this
      * cache.
      *
      * @param topic
      *   Topic being read by [[HeadCacheConsumption]].
      * @param age
      *   Time it took for an element to reach [[HeadCache]].
      * @param diff
      *   The number of elements added to cache by this batch, i.e. returned by
      *   [[PartitionCache#add]].
      */
    def consumer(topic: Topic, age: FiniteDuration, diff: Long): F[Unit]

    /** Report the number of records coming from Cassandra.
      *
      * @param topic
      *   Topic being read by [[Eventual]].
      * @param diff
      *   The number of elements remove from cache by this batch, i.e. returned
      *   by [[PartitionCache#remove]].
      */
    def storage(topic: Topic, diff: Long): F[Unit]
  }

  object Metrics {

    /** Does not do anything, ignores metric reports */
    def empty[F[_]: Applicative]: Metrics[F] = const(().pure[F])

    /** Calls a passed effect when metrics are reported.
      *
      * May only be useful for tests, as the reported parameters are ignored.
      */
    def const[F[_]](unit: F[Unit]): Metrics[F] = {
      class Const
      new Const with Metrics[F] {

        def get(topic: Topic, latency: FiniteDuration, result: String, hit: Boolean) = unit

        def meters(topic: Topic, entries: Int, listeners: Int) = unit

        def consumer(topic: Topic, age: FiniteDuration, diff: Long) = unit

        def storage(topic: Topic, diff: Long) = unit
      }
    }

    type Prefix = String

    object Prefix {
      val default: Prefix = "headcache"
    }

    /** Registers a default set of metrics to a passed collector registry.
      *
      * Note, that creating this metrics several times with the same collector
      * registry may cause errors unless previous [[Metrics]] instance was not
      * released yet.
      *
      * The following metrics will be registered by default, but it is possible
      * to override the default `headcache` prefix to something else.
      *
      * {{{
      * headcache_get_latency HeadCache get latency in seconds
      * headcache_get_result  HeadCache `get` call result counter
      * headcache_entries     HeadCache entries
      * headcache_listeners   HeadCache listeners
      * headcache_records_age HeadCache time difference between record timestamp and now in seconds
      * headcache_diff        HeadCache offset difference between state and source
      * }}}
      *
      * @param registry
      *   smetrics collector registry.
      * @param prefix
      *   Prefix to use for the registered metrics.
      */
    def of[F[_]: Monad](
      registry: CollectorRegistry[F],
      prefix: Prefix = Prefix.default,
    ): Resource[F, Metrics[F]] = {

      val getLatencySummary = registry.summary(
        name      = s"${prefix}_get_latency",
        help      = "HeadCache get latency in seconds",
        quantiles = Quantiles.Default,
        labels    = LabelNames("topic", "result", "now"),
      )

      val getResultCounter = registry.counter(
        name   = s"${prefix}_get_result",
        help   = "HeadCache `get` call result counter",
        labels = LabelNames("topic", "result", "now"),
      )

      val entriesGauge = registry.gauge(name = s"${prefix}_entries", help = "HeadCache entries", labels = LabelNames("topic"))

      val listenersGauge =
        registry.gauge(name = s"${prefix}_listeners", help = "HeadCache listeners", labels = LabelNames("topic"))

      val ageSummary = registry.summary(
        name      = s"${prefix}_records_age",
        help      = "HeadCache time difference between record timestamp and now in seconds",
        quantiles = Quantiles.Default,
        labels    = LabelNames("topic"),
      )

      val diffSummary = registry.summary(
        name      = s"${prefix}_diff",
        help      = "HeadCache offset difference between state and source",
        quantiles = Quantiles.Default,
        labels    = LabelNames("topic", "source"),
      )

      for {
        getLatencySummary <- getLatencySummary
        getResultCounter  <- getResultCounter
        entriesGauge      <- entriesGauge
        listenersGauge    <- listenersGauge
        ageSummary        <- ageSummary
        diffSummary       <- diffSummary
      } yield {

        class Main
        new Main with Metrics[F] {

          def get(topic: Topic, latency: FiniteDuration, result: String, now: Boolean) = {
            val nowLabel = if (now) "now" else "later"
            for {
              _ <- getLatencySummary
                .labels(topic, result, nowLabel)
                .observe(latency.toNanos.nanosToSeconds)
              a <- getResultCounter
                .labels(topic, result, nowLabel)
                .inc()
            } yield a
          }

          def meters(topic: Topic, entries: Int, listeners: Int) = {
            for {
              _ <- listenersGauge
                .labels(topic)
                .set(listeners.toDouble)
              a <- entriesGauge
                .labels(topic)
                .set(entries.toDouble)
            } yield a
          }

          def consumer(topic: Topic, age: FiniteDuration, diff: Long) = {
            for {
              _ <- ageSummary
                .labels(topic)
                .observe(age.toNanos.nanosToSeconds)
              a <- diffSummary
                .labels(topic, "kafka")
                .observe(diff.toDouble)
            } yield a
          }

          def storage(topic: Topic, diff: Long) = {
            diffSummary
              .labels(topic, "storage")
              .observe(diff.toDouble)
          }
        }
      }
    }
  }
}
