package com.evolutiongaming.kafka.journal

import cats.Parallel
import cats.effect.{Concurrent, Resource, Sync, Timer}
import cats.effect.syntax.all._
import cats.syntax.all._
import com.evolution.resourcepool.ResourcePool.implicits._
import com.evolutiongaming.catshelper.{MeasureDuration, Runtime}
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.kafka.journal.Journal.ConsumerPoolConfig
import com.evolutiongaming.kafka.journal.Journals.Consumer

import java.util.concurrent.TimeoutException
import scala.concurrent.duration.{DurationInt, FiniteDuration}

private[journal] object ConsumerPool {

  /**
   * @return The outer Resource is for the pool, the inner is for consumers
   */
  def of[F[_]: Concurrent: Timer: Runtime: MeasureDuration: Parallel](
    poolConfig: ConsumerPoolConfig,
    metrics: Option[ConsumerPoolMetrics[F]],
    consumer: Resource[F, Consumer[F]],
    timeout: FiniteDuration = 1.minute
  ): Resource[F, Resource[F, Consumer[F]]] = {
    for {
      cores <- Runtime[F].availableCores.toResource
      pool  <- consumer.toResourcePool(
        (cores.toDouble * poolConfig.multiplier)
          .round
          .toInt,
        poolConfig.idleTimeout,
        discardTasksOnRelease = true
      )
    } yield {
      val consumer = pool
        .get
        .timeoutTo(
          timeout,
          Sync[F].defer {
            val msg = s"failed to acquire consumer within $timeout"
            JournalError(msg, new TimeoutException(msg)).raiseError
          })
      metrics.fold {
        Resource { consumer }
      } { metrics =>
        Resource {
          for {
            duration <- MeasureDuration[F].start
            result   <- consumer
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
