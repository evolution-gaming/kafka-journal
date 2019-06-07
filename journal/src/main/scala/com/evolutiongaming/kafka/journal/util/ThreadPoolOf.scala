package com.evolutiongaming.kafka.journal.util

import java.util.concurrent.{SynchronousQueue, ThreadFactory, ThreadPoolExecutor}

import cats.effect.{Resource, Sync}
import cats.implicits._

import scala.concurrent.duration._

object ThreadPoolOf {

  def apply[F[_] : Sync](
    minSize: Int,
    maxSize: Int,
    threadFactory: ThreadFactory,
    keepAlive: FiniteDuration = 1.minute,
  ): Resource[F, ThreadPoolExecutor] = {

    val result = for {
      result <- Sync[F].delay {
        new ThreadPoolExecutor(
          minSize,
          maxSize,
          keepAlive.length,
          keepAlive.unit,
          new SynchronousQueue[Runnable],
          threadFactory)
      }
    } yield {
      val release = Sync[F].delay { result.shutdown() }
      (result, release)
    }

    Resource(result)
  }
}
