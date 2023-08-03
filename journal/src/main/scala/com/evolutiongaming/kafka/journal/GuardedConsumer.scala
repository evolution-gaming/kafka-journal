package com.evolutiongaming.kafka.journal

import cats.effect._
import cats.effect.syntax.all._
import cats.effect.std._
import cats.syntax.all._
import com.evolutiongaming.catshelper._
import com.evolutiongaming.skafka.Topic

import scala.concurrent.duration.FiniteDuration

object GuardedConsumer {

  /** Guarantees that no more than `semaphore.maxPermits` consumers exist simultaneously.
    */
  def of[F[_]: Temporal](
    consumer: Resource[F, Journals.Consumer[F]],
    semaphore: Semaphore[F],
    acquireTimeout: FiniteDuration,
    metrics: ConsumerSemaphoreMetrics[F],
    topic: Topic,
  ): Resource[F, Journals.Consumer[F]] = {

    def acquire: F[F[FiniteDuration]] = {
      val acquireWithTimeout = semaphore
        .acquire
        .timeoutTo(
          acquireTimeout,
          Concurrent[F].raiseError(new Exception("Timeout when trying to acquire a consumer"))
        )

      for {
        d       <- MeasureDuration[F].start
        _       <- acquireWithTimeout
        d       <- d
        _       <- metrics.acquireTime(topic, d)
        useTime <- MeasureDuration[F].start
      } yield useTime
    }

    def release(useTime: F[FiniteDuration]): F[Unit] =
      for {
        _       <- semaphore.release
        useTime <- useTime
        _       <- metrics.useTime(topic, useTime)
      } yield ()

    Resource.make(acquire)(release).flatMap(_ => consumer)
  }
}
