package com.evolutiongaming.kafka.journal

import cats.effect._
import cats.effect.syntax.all._
import cats.syntax.all._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.journal.Journal.ConsumerPoolConfig
import com.evolutiongaming.kafka.journal.Journals.Consumer
import com.evolutiongaming.kafka.journal.util.ResourcePool

import scala.concurrent.duration._

private[journal] object ConsumerPool {

  /**
   * @return The outer Resource is for the pool, the inner is for consumers
   */
  def of[F[_]: Async](
    poolConfig: ConsumerPoolConfig,
    metrics: Option[ConsumerPoolMetrics[F]],
    log: Log[F],
  )(
    consumer: Resource[F, Consumer[F]],
  ): Resource[F, Resource[F, Consumer[F]]] = {

    def borrowWithMetrics(pool: ResourcePool[F, Consumer[F]]): Resource[F, Consumer[F]] = {
      metrics match {
        case None =>
          pool.borrow
        case Some(metrics) =>
          for {
            startedAcquiringAt <- Clock[F].monotonic.toResource
            deferred <- Deferred[F, FiniteDuration].toResource
            consumer <- pool.borrow.onFinalize {
              for {
                start <- deferred.get
                end <- Clock[F].monotonic
                _ <- metrics.useTime(end - start)
              } yield ()
            }
            acquiredAt <- Clock[F].monotonic.toResource
            _ <- deferred.complete(acquiredAt).toResource
            _ <- metrics
              .acquireTime(acquiredAt - startedAcquiringAt)
              .toResource
          } yield consumer
      }
    }

    for {
      poolSize <- poolConfig.calculateSize.toResource
      _ <- log.info(s"Creating consumer pool of size $poolSize").toResource
      pool <- ResourcePool.fixedSize[F, Consumer[F]](
        poolSize,
        acquireTimeout = poolConfig.acquireTimeout,
        idleTimeout = poolConfig.idleTimeout,
        log = log,
      )(
        consumer
      )
    } yield borrowWithMetrics(pool)
  }
}
