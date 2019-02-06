package com.evolutiongaming.kafka.journal

import cats.effect.{Fiber, Resource, Sync}
import cats.implicits._

object ResourceOf {

  def apply[F[_] : Sync, A](fiber: F[Fiber[F, A]]): Resource[F, F[A]] = {
    val result = for {
      fiber <- fiber
    } yield {
      (fiber.join, fiber.cancel)
    }
    Resource(result)
  }
}
