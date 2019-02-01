package com.evolutiongaming.kafka.journal

import cats.effect.Sync
import com.evolutiongaming.kafka.journal.HeadCache.Metrics.Result
import com.evolutiongaming.kafka.journal.MetricsHelper._
import com.evolutiongaming.skafka.Topic
import io.prometheus.client.{CollectorRegistry, Counter, Gauge, Summary}

object HeadCacheMetrics {

  def of[F[_] : Sync](
    registry: CollectorRegistry,
    prefix: String = "headcache"): F[HeadCache.Metrics[F]] = {

    Sync[F].delay {

      val getLatencySummary = Summary.build()
        .name(s"${ prefix }_get_latency")
        .help("HeadCache get latency in seconds")
        .labelNames("topic", "result")
        .quantile(0.5, 0.05)
        .quantile(0.9, 0.05)
        .quantile(0.95, 0.01)
        .quantile(0.99, 0.005)
        .register(registry)

      val getResultCounter = Counter.build()
        .name(s"${ prefix }_get_results")
        .help("HeadCache `get` call result: replicated, not_replicated, invalid or failure")
        .labelNames("topic", "result")
        .register(registry)

      val entriesGauge = Gauge.build()
        .name(s"${ prefix }_entries")
        .help("HeadCache entries")
        .labelNames("topic")
        .register(registry)

      val listenersGauge = Gauge.build()
        .name(s"${ prefix }_listeners")
        .help("HeadCache listeners")
        .labelNames("topic")
        .register(registry)

      val deliveryLatencySummary = Summary.build()
        .name(s"${ prefix }_delivery_latency")
        .help("HeadCache kafka delivery latency in seconds")
        .labelNames("topic")
        .quantile(0.5, 0.05)
        .quantile(0.9, 0.05)
        .quantile(0.95, 0.01)
        .quantile(0.99, 0.005)
        .register(registry)

      new HeadCache.Metrics[F] {

        def get(topic: Topic, latency: Long, result: Result) = {

          val name = result match {
            case Result.Replicated    => "replicated"
            case Result.NotReplicated => "not_replicated¨"
            case Result.Invalid       => "invalid"
            case Result.Failure       => "failure"
          }

          Sync[F].delay {

            getLatencySummary
              .labels(topic, name)
              .observe(latency.toSeconds)

            getResultCounter
              .labels(topic, name)
              .inc()
          }
        }

        def listeners(topic: Topic, size: Int) = {
          Sync[F].delay {
            listenersGauge
              .labels(topic)
              .set(size.toDouble)
          }
        }

        def round(topic: Topic, entries: Long, listeners: Int, deliveryLatency: Long) = {
          Sync[F].delay {

            entriesGauge
              .labels(topic)
              .set(entries.toDouble)

            listenersGauge
              .labels(topic)
              .set(listeners.toDouble)

            deliveryLatencySummary
              .labels(topic)
              .observe(deliveryLatency.toSeconds)
          }
        }
      }
    }
  }
}
