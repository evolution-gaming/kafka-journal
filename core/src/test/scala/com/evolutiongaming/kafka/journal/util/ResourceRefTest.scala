package com.evolutiongaming.kafka.journal.util

import cats.effect.{Ref, *}
import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.IOSuite.*
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

class ResourceRefTest extends AsyncFunSuite with Matchers {

  test("ResourceRef") {

    def resourceOf[A](a: A, ref: Ref[IO, Boolean]) = {
      Resource.make { ref.set(true).as(a) } { _ => ref.set(false) }
    }

    val result = for {
      ref0 <- Ref[IO].of(false)
      ref1 <- Ref[IO].of(false)
      ref <- ResourceRef.make(resourceOf(0, ref0)).use { ref =>
        for {
          a <- ref.get
          _ = a shouldEqual 0
          a <- ref0.get
          _ = a shouldEqual true
          _ <- ref.set(resourceOf(1, ref1))
          a <- ref.get
          _ = a shouldEqual 1
          a <- ref0.get
          _ = a shouldEqual false
          a <- ref1.get
          _ = a shouldEqual true
        } yield ref
      }
      a <- ref1.get
      _ = a shouldEqual false
      _ <- ().pure[IO]
      a <- ref.get.attempt
      _ = a shouldEqual ResourceReleasedError.asLeft
    } yield {}
    result.run()
  }
}
