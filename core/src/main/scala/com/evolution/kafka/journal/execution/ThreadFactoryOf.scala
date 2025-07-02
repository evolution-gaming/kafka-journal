package com.evolution.kafka.journal.execution

import cats.effect.Sync
import cats.syntax.all.*

import java.util.concurrent.{Executors as ExecutorsJ, ThreadFactory}

private[journal] object ThreadFactoryOf {

  def apply[F[_]: Sync](
    prefix: String,
    uncaughtExceptionHandler: Thread.UncaughtExceptionHandler = UncaughtExceptionHandler.default,
  ): F[ThreadFactory] = {

    for {
      factory <- Sync[F].delay { ExecutorsJ.defaultThreadFactory() }
    } yield {
      new ThreadFactory {
        def newThread(runnable: Runnable) = {
          val thread = factory.newThread(runnable)
          val threadId = thread.getId
          thread.setName(s"$prefix-$threadId")
          thread.setUncaughtExceptionHandler(uncaughtExceptionHandler)
          thread
        }
      }
    }
  }
}
