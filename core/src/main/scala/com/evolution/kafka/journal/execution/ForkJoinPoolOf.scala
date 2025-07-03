package com.evolution.kafka.journal.execution

import cats.effect.{Resource, Sync}
import cats.syntax.all.*

import java.util.concurrent.{ForkJoinPool, ForkJoinWorkerThread}
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory

private[journal] object ForkJoinPoolOf {

  def apply[F[_]: Sync](
    name: String,
    parallelism: Int,
  ): Resource[F, ForkJoinPool] = {

    val threadFactory = ForkJoinPool.defaultForkJoinWorkerThreadFactory.withPrefix(name)

    val threadPool = Sync[F].delay {
      new ForkJoinPool(parallelism, threadFactory, UncaughtExceptionHandler.default, true)
    }

    val result = for {
      threadPool <- threadPool
    } yield {
      val release = Sync[F].delay { threadPool.shutdown() }
      (threadPool, release)
    }

    Resource(result)
  }

  implicit class ForkJoinWorkerThreadFactoryOps(val self: ForkJoinWorkerThreadFactory) extends AnyVal {

    def withPrefix(prefix: String): ForkJoinWorkerThreadFactory = new ForkJoinWorkerThreadFactory {

      def newThread(pool: ForkJoinPool): ForkJoinWorkerThread = {
        val thread = self.newThread(pool)
        val threadId = thread.getId
        thread.setName(s"$prefix-$threadId")
        thread
      }
    }
  }
}
