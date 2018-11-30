package com.evolutiongaming.kafka.journal.util

import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import org.scalatest.Succeeded

import scala.concurrent.{ExecutionContext, Future}

object IOSuite {
  implicit val ec: ExecutionContext = ExecutionContext.Implicits.global
  implicit val timer: Timer[IO] = IO.timer(ec)
  implicit val cs: ContextShift[IO] = IO.contextShift(ec)

  def runIO[A](io: IO[A]): Future[Succeeded.type] = io.as(Succeeded).unsafeToFuture

  implicit class IOOps[A](val self: IO[A]) extends AnyVal {
    def run: Future[Succeeded.type] = runIO(self)
  }
}