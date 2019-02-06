package com.evolutiongaming.kafka.journal

import cats.effect.concurrent.{Deferred, Ref}
import cats.effect._
import cats.implicits._
import com.evolutiongaming.kafka.journal.IOSuite._
import org.scalatest.{AsyncFunSuite, Matchers}

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
