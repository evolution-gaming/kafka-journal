package com.evolutiongaming.kafka.journal.util

import java.util.concurrent.{ScheduledExecutorService, ThreadFactory, Executors => ExecutorsJ}

import cats.effect.{Resource, Sync}
import cats.implicits._


object ScheduledExecutorServiceOf {

  def apply[F[_] : Sync](prefix: String, parallelism: Int): Resource[F, ScheduledExecutorService] = {

    val factory = {
      val factory = ExecutorsJ.defaultThreadFactory()
      new ThreadFactory {
        def newThread(runnable: Runnable) = {
          val thread = factory.newThread(runnable)
          val threadId = thread.getId
          thread.setName(s"$prefix-$threadId")
          thread
        }
      }
    }

    val result = for {
      threadPool <- Sync[F].delay { ExecutorsJ.newScheduledThreadPool(parallelism, factory) }
    } yield {
      val release = Sync[F].delay { threadPool.shutdown() }
      (threadPool, release)
    }
    Resource(result)
  }
}
