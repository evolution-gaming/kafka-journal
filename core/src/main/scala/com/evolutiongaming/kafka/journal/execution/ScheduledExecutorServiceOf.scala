package com.evolutiongaming.kafka.journal.execution

import cats.effect.{Resource, Sync}
import cats.syntax.all.*

import java.util.concurrent.{ScheduledExecutorService, ThreadFactory, Executors as ExecutorsJ}

private[journal] object ScheduledExecutorServiceOf {

  def apply[F[_]: Sync](
    parallelism: Int,
    threadFactory: ThreadFactory,
  ): Resource[F, ScheduledExecutorService] = {

    val result = for {
      threadPool <- Sync[F].delay { ExecutorsJ.newScheduledThreadPool(parallelism, threadFactory) }
    } yield {
      val release = Sync[F].delay { threadPool.shutdown() }
      (threadPool, release)
    }
    Resource(result)
  }
}
