package com.evolutiongaming.kafka.journal.util

import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory
import java.util.concurrent.ForkJoinPool

import cats.effect.{Resource, Sync}
import cats.implicits._
import com.evolutiongaming.catshelper.{Log, ToFuture}


object ForkJoinPoolOf {

  def apply[F[_] : Sync : ToFuture](
    name: String,
    parallelism: Int,
    log: Log[F]

  ): Resource[F, ForkJoinPool] = {

    val threadFactory = ForkJoinPool.defaultForkJoinWorkerThreadFactory.withPrefix(name)

    val exceptionHandler = new Thread.UncaughtExceptionHandler {
      def uncaughtException(thread: Thread, exception: Throwable) = {
        ToFuture[F].apply { log.error(s"uncaught exception on $thread: $exception", exception) }
        ()
      }
    }

    val threadPool = Sync[F].delay {
      new ForkJoinPool(
        parallelism,
        threadFactory,
        exceptionHandler,
        true)
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

      def newThread(pool: ForkJoinPool) = {
        val thread = self.newThread(pool)
        val threadId = thread.getId
        thread.setName(s"$prefix-$threadId")
        thread
      }
    }
  }
}
