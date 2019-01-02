package com.evolutiongaming.kafka.journal.util

import cats.effect.concurrent.{Deferred, Ref}
import cats.effect._
import cats.implicits._
import com.evolutiongaming.kafka.journal.util.IOSuite._
import org.scalatest.{AsyncFunSuite, Matchers}

class StartResSpec extends AsyncFunSuite with Matchers {

  test("StartRes") {
    val result = for {
      deferred <- Deferred[IO, Unit]
      ref      <- Ref.of[IO, Boolean](false)
      res       = Resource.make(IO.unit)(_ => ref.set(true))
      fiber    <- StartRes(res)(_ => deferred.complete(()) *> IO.never.as(()))
      _        <- deferred.get
      _        <- fiber.cancel
      result   <- ref.get
    } yield {
      result shouldEqual true
    }
    result.run()
  }
}
