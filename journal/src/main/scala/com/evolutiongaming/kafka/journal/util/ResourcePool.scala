package com.evolutiongaming.kafka.journal.util

import cats.effect.implicits.effectResourceOps
import cats.effect.std.Queue
import cats.effect.{Concurrent, Resource}
import cats.syntax.all._

private[journal] trait ResourcePool[F[_], O] {

  /**
   * `acquire` function of the Resource semantically blocks if there are no objects available.
   *
   * `release` function of the Resource returns the object to the pool.
   * Note that it will NOT deallocate the object.
   */
  def borrow: Resource[F, O]
}

private[journal] object ResourcePool {

  /**
   *  Objects are allocated when a pool is created and deallocated when it shuts down.
   */
  def fixedSize[F[_]: Concurrent, O](objectOf: Resource[F, O], poolSize: Int): Resource[F, ResourcePool[F, O]] = {
    for {
      objects <- Vector.fill(poolSize)(objectOf).sequence
      unused <- Queue.bounded[F, O](poolSize).toResource
      _ <- objects.traverse(unused.offer).toResource
    } yield {
      class Main
      new Main with ResourcePool[F, O] {
        override def borrow: Resource[F, O] = Resource.make(unused.take)(unused.offer)
      }
    }
  }
}
