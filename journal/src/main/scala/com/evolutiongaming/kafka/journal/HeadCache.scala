package com.evolutiongaming.kafka.journal

import cats._
import cats.effect._
import cats.effect.concurrent.Ref
import cats.syntax.all._
import com.evolutiongaming.catshelper._
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.kafka.journal.PartitionCache.Result
import com.evolutiongaming.kafka.journal.conversions.ConsRecordToActionHeader
import com.evolutiongaming.kafka.journal.eventual.{EventualJournal, TopicPointers}
import com.evolutiongaming.kafka.journal.util.CacheHelper._
import com.evolutiongaming.scache.{Cache, ExpiringCache}
import com.evolutiongaming.skafka.consumer.ConsumerConfig
import com.evolutiongaming.skafka.{Offset, Partition, Topic}
import com.evolutiongaming.smetrics.MetricsHelper._
import com.evolutiongaming.smetrics._

import scala.concurrent.duration._

/**
 * TODO headcache:
 * 1. Keep 1000 last seen entries, even if replicated.
 * 2. Fail headcache when background tasks failed
 */
trait HeadCache[F[_]] {

  def get(key: Key, partition: Partition, offset: Offset): F[Option[HeadInfo]]
}


object HeadCache {

  def empty[F[_]: Applicative]: HeadCache[F] = const(none[HeadInfo].pure[F])


  def const[F[_]](value: F[Option[HeadInfo]]): HeadCache[F] = {
    class Const
    new Const with HeadCache[F] {
      def get(key: Key, partition: Partition, offset: Offset) = value
    }
  }


  def of[F[_]: Concurrent: Parallel: Timer: Runtime: LogOf: KafkaConsumerOf: MeasureDuration: FromTry: FromJsResult: JsonCodec.Decode](
    consumerConfig: ConsumerConfig,
    eventualJournal: EventualJournal[F],
    metrics: Option[HeadCacheMetrics[F]]
  ): Resource[F, HeadCache[F]] = {
    for {
      log    <- LogOf[F].apply(HeadCache.getClass).toResource
      result <- HeadCache.of(
        Eventual(eventualJournal),
        log,
        TopicCache.Consumer.of[F](consumerConfig),
        metrics)
      result <- result.withFence
    } yield {
      result.withLog(log)
    }
  }


  def of[F[_]: Concurrent: Parallel: Timer: Runtime: FromJsResult: MeasureDuration: JsonCodec.Decode](
    eventual: Eventual[F],
    log: Log[F],
    consumer: Resource[F, TopicCache.Consumer[F]],
    metrics: Option[HeadCacheMetrics[F]],
    config: HeadCacheConfig = HeadCacheConfig.default
  ): Resource[F, HeadCache[F]] = {

    val consRecordToActionHeader = ConsRecordToActionHeader[F]
    for {
      cache <- Cache.expiring(ExpiringCache.Config[F, Topic, TopicCache[F]](expireAfterRead = config.expiry))
      cache <- metrics.fold(cache.pure[Resource[F, *]]) { metrics => cache.withMetrics(metrics.cache) }
    } yield {
      class Main
      new Main with HeadCache[F] {

        def get(key: Key, partition: Partition, offset: Offset) = {
          val topic = key.topic
          val log1 = log.prefixed(topic)
          cache
            .getOrUpdateResource(topic) {
              TopicCache
                .of(
                  eventual,
                  topic,
                  log1,
                  consumer,
                  config,
                  consRecordToActionHeader,
                  metrics.map { _.headCache })
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


  trait Eventual[F[_]] {

    def pointers(topic: Topic): F[TopicPointers]
  }

  object Eventual {

    def apply[F[_]](implicit F: Eventual[F]): Eventual[F] = F

    def apply[F[_]](eventualJournal: EventualJournal[F]): Eventual[F] = {
      class Main
      new Main with HeadCache.Eventual[F] {
        def pointers(topic: Topic) = eventualJournal.pointers(topic)
      }
    }

    def empty[F[_]: Applicative]: Eventual[F] = const(TopicPointers.empty.pure[F])

    def const[F[_]](value: F[TopicPointers]): Eventual[F] = {
      class Const
      new Const with Eventual[F] {
        def pointers(topic: Topic) = value
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

    def withLog(log: Log[F])(implicit F: FlatMap[F], measureDuration: MeasureDuration[F]): HeadCache[F] = {
      new WithLog with HeadCache[F] {

        def get(key: Key, partition: Partition, offset: Offset) = {
          for {
            d <- MeasureDuration[F].start
            a <- self.get(key, partition, offset)
            d <- d
            _ <- log.debug(s"get in ${ d.toMillis }ms, key: $key, offset: $partition:$offset, result: $a")
          } yield a
        }
      }
    }

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


  trait Metrics[F[_]] {

    def get(topic: Topic, latency: FiniteDuration, result: String, now: Boolean): F[Unit]

    def meters(topic: Topic, entries: Int, listeners: Int): F[Unit]

    def consumer(topic: Topic, age: FiniteDuration, diff: Long): F[Unit]

    def storage(topic: Topic, diff: Long): F[Unit]
  }

  object Metrics {

    def empty[F[_]: Applicative]: Metrics[F] = const(().pure[F])


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


    def of[F[_]: Monad](
      registry: CollectorRegistry[F],
      prefix: Prefix = Prefix.default
    ): Resource[F, Metrics[F]] = {

      val getLatencySummary = registry.summary(
        name = s"${ prefix }_get_latency",
        help = "HeadCache get latency in seconds",
        quantiles = Quantiles.Default,
        labels = LabelNames("topic", "result", "now"))

      val getResultCounter = registry.counter(
        name = s"${ prefix }_get_result",
        help = "HeadCache `get` call result counter",
        labels = LabelNames("topic", "result", "now"))

      val entriesGauge = registry.gauge(
        name = s"${ prefix }_entries",
        help = "HeadCache entries",
        labels = LabelNames("topic"))

      val listenersGauge = registry.gauge(
        name = s"${ prefix }_listeners",
        help = "HeadCache listeners",
        labels = LabelNames("topic"))

      val ageSummary = registry.summary(
        name = s"${ prefix }_records_age",
        help = "HeadCache time difference between record timestamp and now in seconds",
        quantiles = Quantiles.Default,
        labels = LabelNames("topic"))

      val diffSummary = registry.summary(
        name = s"${ prefix }_diff",
        help = "HeadCache offset difference between state and source",
        quantiles = Quantiles.Default,
        labels = LabelNames("topic", "source"))

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