package com.evolutiongaming.kafka.journal.util

import cats.effect.{Deferred, Ref, _}
import com.evolutiongaming.kafka.journal.IOSuite._
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

class StartResourceSpec extends AsyncFunSuite with Matchers {

  test("StartResource") {
    val result = for {
      deferred <- Deferred[IO, Unit]
      ref      <- Ref.of[IO, Boolean](false)
      res       = Resource.make(IO.unit)(_ => ref.set(true))
      fiber    <- StartResource(res)(_ => deferred.complete(()) *> IO.never.as(()))
      _        <- deferred.get
      _        <- fiber.cancel
      result   <- ref.get
    } yield {
      result shouldEqual true
    }
    result.run()
  }
}
