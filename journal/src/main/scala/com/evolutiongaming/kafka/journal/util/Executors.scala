package com.evolutiongaming.kafka.journal.util

import java.util.concurrent.ScheduledExecutorService

import cats.effect.{Resource, Sync}
import com.evolutiongaming.catshelper.{Log, Runtime, ToFuture}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

object Executors {

  def blocking[F[_] : Sync : Runtime : ToFuture](
    name: String,
    log: Log[F]
  ): Resource[F, ExecutionContextExecutorService] = {
    for {
      cores       <- Resource.liftF(Runtime[F].availableCores)
      parallelism  = (cores + 2) * 3
      pool        <- forkJoin(name, parallelism, log)
    } yield pool
  }


  def nonBlocking[F[_] : Sync : Runtime : ToFuture](
    name: String,
    log: Log[F]
  ): Resource[F, ExecutionContextExecutorService] = {
    for {
      cores       <- Resource.liftF(Runtime[F].availableCores)
      parallelism  = cores + 4
      pool        <- forkJoin(name, parallelism, log)
    } yield pool
  }


  def forkJoin[F[_] : Sync : ToFuture](
    name: String,
    parallelism: Int,
    log: Log[F]
  ): Resource[F, ExecutionContextExecutorService] = {
    for {
      pool <- ForkJoinPoolOf[F](name, parallelism, log)
    } yield {
      ExecutionContext.fromExecutorService(pool)
    }
  }


  def scheduled[F[_] : Sync](name: String, parallelism: Int): Resource[F, ScheduledExecutorService] = {
    ScheduledExecutorServiceOf[F](name, parallelism)
  }
}