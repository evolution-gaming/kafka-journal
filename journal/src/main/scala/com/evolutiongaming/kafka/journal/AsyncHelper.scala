package com.evolutiongaming.kafka.journal

import com.evolutiongaming.concurrent.async.Async

import scala.collection.generic.CanBuildFrom

object AsyncHelper {
  implicit class AsyncOps(val self: Async.type) extends AnyVal {

    def seq[A, M[X] <: TraversableOnce[X]](as: M[Async[A]])(implicit cbf: CanBuildFrom[M[Async[A]], A, M[A]]): Async[M[A]] = {
      val b = Async(cbf(as))
      as.foldLeft(b) { (b, a) =>
        for {
          b <- b
          a <- a
        } yield {
          b += a
        }
      }.map(_.result())
    }

    def list[A](as: List[Async[A]]): Async[List[A]] = {
      val b = Async(List.empty[A])
      for {
        list <- as.foldLeft(b) { (b, a) =>
          for {
            b <- b
            a <- a
          } yield {
            a :: b
          }
        }
      } yield list.reverse
    }
  }
}