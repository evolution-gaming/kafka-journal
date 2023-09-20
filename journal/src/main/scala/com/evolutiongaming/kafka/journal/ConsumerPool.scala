package com.evolutiongaming.kafka.journal

import cats.effect.{Concurrent, Resource, Timer}
import cats.syntax.all._
import com.evolution.resourcepool.ResourcePool
import com.evolutiongaming.catshelper.{MeasureDuration, Runtime}
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.kafka.journal.Journal.ConsumerPoolConfig
import com.evolutiongaming.kafka.journal.Journals.Consumer

private[journal] object ConsumerPool {

  /**
   * @return The outer Resource is for the pool, the inner is for consumers
   */
  def of[F[_]: Concurrent: Timer: Runtime: MeasureDuration](
    poolConfig: ConsumerPoolConfig,
    metrics: Option[ConsumerPoolMetrics[F]],
    consumer: Resource[F, Consumer[F]],
  ): Resource[F, Resource[F, Consumer[F]]] = {

    for {
      cores <- Runtime[F].availableCores.toResource
      pool  <- ResourcePool.of(
        (cores.toDouble * poolConfig.multiplier)
          .round
          .toInt,
        poolConfig.idleTimeout,
        _ => consumer
      )
    } yield {
      metrics.fold {
        pool.resource
      } { metrics =>
        Resource {
          for {
            duration <- MeasureDuration[F].start
            result   <- pool.get
            duration <- duration
            _        <- metrics.acquire(duration)
            duration <- MeasureDuration[F].start
          } yield {
            val (consumer, release) = result
            val release1 = for {
              duration <- duration
              _        <- metrics.use(duration)
              result   <- release
            } yield result
            (consumer, release1)
          }
        }
      }
    }
  }
}
