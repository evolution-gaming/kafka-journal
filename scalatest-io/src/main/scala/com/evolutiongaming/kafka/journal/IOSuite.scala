package com.evolutiongaming.kafka.journal

import cats.effect.unsafe.implicits.global
import cats.effect.{Clock, IO}
import com.evolutiongaming.smetrics.MeasureDuration
import org.scalatest.Succeeded

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

object IOSuite {
  val Timeout: FiniteDuration = 5.seconds

  implicit val executor: ExecutionContextExecutor = ExecutionContext.global
  implicit val measureDuration: MeasureDuration[IO] = MeasureDuration.fromClock(Clock[IO])

  def runIO[A](io: IO[A], timeout: FiniteDuration = Timeout): Future[Succeeded.type] = {
    io.timeout(timeout).as(Succeeded).unsafeToFuture()
  }

  implicit class IOOps[A](val self: IO[A]) extends AnyVal {
    def run(timeout: FiniteDuration = Timeout): Future[Succeeded.type] = runIO(self, timeout)
  }
}
