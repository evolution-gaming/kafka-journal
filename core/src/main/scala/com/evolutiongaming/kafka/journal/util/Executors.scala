package com.evolutiongaming.kafka.journal.util

import cats.effect.syntax.resource.*
import cats.effect.{Resource, Sync}
import com.evolutiongaming.catshelper.Runtime
import com.evolutiongaming.kafka.journal.execution.{ForkJoinPoolOf, ScheduledExecutorServiceOf, ThreadFactoryOf, ThreadPoolOf}

import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

private[journal] object Executors {

  def blocking[F[_]: Sync](
    name: String,
  ): Resource[F, ExecutionContextExecutorService] = {
    for {
      threadFactory <- ThreadFactoryOf[F](name).toResource
      threadPool    <- ThreadPoolOf[F](2, Int.MaxValue, threadFactory)
    } yield {
      ExecutionContext.fromExecutorService(threadPool)
    }
  }

  def nonBlocking[F[_]: Sync](
    name: String,
  ): Resource[F, ExecutionContextExecutorService] = {
    for {
      cores        <- Runtime[F].availableCores.toResource
      parallelism   = cores + 1
      forkJoinPool <- ForkJoinPoolOf[F](name, parallelism)
    } yield {
      ExecutionContext.fromExecutorService(forkJoinPool)
    }
  }

  def scheduled[F[_]: Sync](
    name: String,
    parallelism: Int,
  ): Resource[F, ScheduledExecutorService] = {
    for {
      threadFactory <- ThreadFactoryOf[F](name).toResource
      result        <- ScheduledExecutorServiceOf[F](parallelism, threadFactory)
    } yield result
  }
}
