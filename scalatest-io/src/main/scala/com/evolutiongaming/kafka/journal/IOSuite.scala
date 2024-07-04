package com.evolutiongaming.kafka.journal

import cats.effect.unsafe.implicits.global
import cats.effect.{Clock, IO}
import com.evolutiongaming.catshelper.MeasureDuration
import org.scalatest.Succeeded

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

object IOSuite {
  val Timeout: FiniteDuration = 1.minute

  implicit val executor: ExecutionContextExecutor   = ExecutionContext.global
  implicit val measureDuration: MeasureDuration[IO] = MeasureDuration.fromClock(Clock[IO])

  def runIO[A](io: IO[A], timeout: FiniteDuration = Timeout): Future[Succeeded.type] = {
    io
      .timeout(timeout)
      .as(Succeeded)
      .unsafeToFuture()
  }

  implicit class IOOps[A](val self: IO[A]) extends AnyVal {
    def run(timeout: FiniteDuration = Timeout): Future[Succeeded.type] = runIO(self, timeout)

    def eventually: IO[A] = {
      def attempt: IO[A] = self.attempt.flatMap {
        case Left(_)  => IO.sleep(10.millis) >> attempt
        case Right(a) => IO.pure(a)
      }

      attempt
    }
  }
}
