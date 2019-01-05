package com.evolutiongaming.kafka.journal.util

import cats.effect.{Concurrent, ContextShift, IO, Timer}
import cats.implicits._
import org.scalatest.Succeeded
import com.evolutiongaming.kafka.journal.util.CatsHelper._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

object IOSuite {
  val Timeout: FiniteDuration = 5.seconds
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global
  implicit val csIO: ContextShift[IO] = IO.contextShift(ec)
  implicit val concurrentIO: Concurrent[IO] = IO.ioConcurrentEffect(csIO)
  implicit val timerIO: Timer[IO] = IO.timer(ec)
  implicit val fromFutureIO: FromFuture[IO] = FromFuture.lift[IO]

  def runIO[A](io: IO[A], timeout: FiniteDuration = Timeout): Future[Succeeded.type] = {
    io.timeout1(timeout).as(Succeeded).unsafeToFuture
  }

  implicit class IOOps[A](val self: IO[A]) extends AnyVal {
    def run(timeout: FiniteDuration = Timeout): Future[Succeeded.type] = runIO(self, timeout)
  }
}