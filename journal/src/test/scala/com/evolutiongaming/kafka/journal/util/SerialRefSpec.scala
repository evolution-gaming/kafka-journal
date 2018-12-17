package com.evolutiongaming.kafka.journal.util

import cats._
import cats.effect.IO
import cats.implicits._
import com.evolutiongaming.kafka.journal.util.IOSuite._
import org.scalatest.AsyncFunSuite

import scala.concurrent.duration._

class SerialRefSpec extends AsyncFunSuite {

  test("modify") {
    val delay = timer.sleep(10.millis)

    def expect[A: Eq](t: IO[A], expected: A): IO[Unit] = {
      for {
        a <- t
        r <- {
          if (Eq[A].eqv(a, expected)) IO.unit
          else delay *> expect(t, expected)
        }
      } yield {
        r
      }
    }

    val result = for {
      ref <- SerialVar.of[IO, Int](0)
      expected = 1000
      modifies = List.fill(expected)(IO.shift *> ref.update(x => IO.delay { x + 1 })).parSequence
      result <- IO.shift *> modifies.start *> expect(ref.get, expected)
    } yield {
      result
    }

    result.run
  }
}
