package com.evolutiongaming.kafka.journal.util

import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import org.scalatest.Succeeded

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

object IOSuite {
  implicit lazy val ec: ExecutionContextExecutor = ExecutionContext.global
  implicit lazy val cs: ContextShift[IO] = IO.contextShift(ec)
  implicit lazy val timer: Timer[IO] = IO.timer(ec)

  def runIO[A](io: IO[A]): Future[Succeeded.type] = io.as(Succeeded).unsafeToFuture

  implicit class IOOps[A](val self: IO[A]) extends AnyVal {
    def run: Future[Succeeded.type] = runIO(self)
  }
}