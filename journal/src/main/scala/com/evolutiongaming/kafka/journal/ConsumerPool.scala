package com.evolutiongaming.kafka.journal

import cats.effect.*
import cats.effect.kernel.Resource.ExitCase
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.evolution.resourcepool.ResourcePool.implicits.*
import com.evolutiongaming.catshelper.{MeasureDuration, Runtime}
import com.evolutiongaming.kafka.journal.Journal.ConsumerPoolConfig
import com.evolutiongaming.kafka.journal.Journals.Consumer

import java.util.concurrent.TimeoutException
import scala.concurrent.duration.{DurationInt, FiniteDuration}

private[journal] object ConsumerPool {

  /**
   * @return
   *   The outer Resource is for the pool, the inner is for consumers
   */
  def make[F[_]: Async: Runtime: MeasureDuration](
    poolConfig: ConsumerPoolConfig,
    metrics: Option[ConsumerPoolMetrics[F]],
    consumer: Resource[F, Consumer[F]],
    timeout: FiniteDuration = 1.minute,
  ): Resource[F, Resource[F, Consumer[F]]] = {
    for {
      cores <- Runtime[F].availableCores.toResource
      pool <- consumer.toResourcePool(
        maxSize = math.max(1, (cores.toDouble * poolConfig.multiplier).intValue),
        expireAfter = poolConfig.idleTimeout,
        discardTasksOnRelease = true,
      )
    } yield {
      Resource.applyFull { poll =>
        poll {
          val consumer = pool
            .get
            .timeoutTo(
              timeout,
              Sync[F].defer {
                val msg = s"failed to acquire consumer within $timeout"
                JournalError(msg, new TimeoutException(msg)).raiseError
              },
            )
          metrics.fold {
            consumer.map { case (consumer, release) => (consumer, (_: ExitCase) => release) }
          } { metrics =>
            for {
              duration <- MeasureDuration[F].start
              result <- consumer.attempt
              duration <- duration
              _ <- metrics.acquire(duration)
              result <- result.liftTo[F]
              duration <- MeasureDuration[F].start
            } yield {
              val (consumer, release) = result
              (
                consumer,
                (_: ExitCase) =>
                  for {
                    duration <- duration
                    _ <- metrics.use(duration)
                    result <- release
                  } yield result,
              )
            }
          }
        }
      }
    }
  }
}
