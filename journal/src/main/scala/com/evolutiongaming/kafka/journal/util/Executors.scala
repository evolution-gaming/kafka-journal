package com.evolutiongaming.kafka.journal.util

import java.util.concurrent.ScheduledExecutorService

import cats.effect.{Resource, Sync}
import com.evolutiongaming.catshelper.Runtime

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

object Executors {

  def blocking[F[_] : Sync](
    name: String,
  ): Resource[F, ExecutionContextExecutorService] = {
    for {
      threadFactory <- Resource.liftF(ThreadFactoryOf[F](name))
      threadPool    <- ThreadPoolOf[F](2, Int.MaxValue, threadFactory)
    } yield {
      ExecutionContext.fromExecutorService(threadPool)
    }
  }


  def nonBlocking[F[_] : Sync](
    name: String,
  ): Resource[F, ExecutionContextExecutorService] = {
    for {
      cores        <- Resource.liftF(Runtime[F].availableCores)
      parallelism   = cores + 4
      forkJoinPool <- ForkJoinPoolOf[F](name, parallelism)
    } yield {
      ExecutionContext.fromExecutorService(forkJoinPool)
    }
  }


  def scheduled[F[_] : Sync](
    name: String,
    parallelism: Int
  ): Resource[F, ScheduledExecutorService] = {
    for {
      threadFactory <- Resource.liftF(ThreadFactoryOf[F](name))
      result        <- ScheduledExecutorServiceOf[F](parallelism, threadFactory)
    } yield result
  }
}