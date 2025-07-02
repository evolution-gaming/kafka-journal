package com.evolution.kafka.journal

import cats.syntax.all.*
import com.evolution.kafka.journal.Bounds.implicits.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.util.{Failure, Try}

class BoundsTest extends AnyFunSuite with Matchers {

  test("Bounds(0, 0)") {
    val result = for {
      a <- Bounds.of[Try](0, 0)
      _ <- Try { a.min shouldEqual 0 }
      _ <- Try { a.max shouldEqual 0 }
    } yield {}
    result shouldEqual ().pure[Try]
  }

  test("Bounds(0, 1)") {
    val result = for {
      a <- Bounds.of[Try](0, 1)
      _ <- Try { a.min shouldEqual 0 }
      _ <- Try { a.max shouldEqual 1 }
    } yield {}
    result shouldEqual ().pure[Try]
  }

  test("Bounds(1, 0)") {
    Bounds.of[Try](1, 0) should matchPattern { case Failure(_: JournalError) => () }
  }

  test("Bounds(0).toString") {
    Bounds(0).toString shouldEqual "0"
  }

  test("Bounds(0, 1).toString") {
    val result = for {
      a <- Bounds.of[Try](0, 1)
      _ <- Try { a.toString shouldEqual "0..1" }
    } yield {}
    result shouldEqual ().pure[Try]
  }

  test("0 > Bounds(0)") {
    0 > Bounds(0) shouldEqual false
  }

  test("0 > Bounds(0, 1)") {
    val result = for {
      a <- Bounds.of[Try](0, 1)
      _ <- Try { 0 > a shouldEqual false }
    } yield {}
    result shouldEqual ().pure[Try]
  }

  test("1 > Bounds(0, 1)") {
    val result = for {
      a <- Bounds.of[Try](0, 1)
      _ <- Try { 1 > a shouldEqual false }
    } yield {}
    result shouldEqual ().pure[Try]
  }

  test("1 > Bounds(0)") {
    1 > Bounds(0) shouldEqual true
  }

  test("2 > Bounds(0, 1)") {
    val result = for {
      a <- Bounds.of[Try](0, 1)
      _ <- Try { 2 > a shouldEqual true }
    } yield {}
    result shouldEqual ().pure[Try]
  }

  test("0 < Bounds(0)") {
    0 < Bounds(0) shouldEqual false
  }

  test("0 < Bounds(0, 1)") {
    val result = for {
      a <- Bounds.of[Try](0, 1)
      _ <- Try { 0 < a shouldEqual false }
    } yield {}
    result shouldEqual ().pure[Try]
  }

  test("1 < Bounds(0, 1)") {
    val result = for {
      a <- Bounds.of[Try](0, 1)
      _ <- Try { 1 < a shouldEqual false }
    } yield {}
    result shouldEqual ().pure[Try]
  }

  test("0 < Bounds(1)") {
    0 < Bounds(1) shouldEqual true
  }

  test("0 < Bounds(1, 2)") {
    val result = for {
      a <- Bounds.of[Try](1, 2)
      _ <- Try { 0 < a shouldEqual true }
    } yield {}
    result shouldEqual ().pure[Try]
  }

  test("Bounds(0) > 0") {
    Bounds.BoundsOps(Bounds(0)) > 0 shouldEqual false
  }

  test("Bounds(0, 1) > 0") {
    val result = for {
      a <- Bounds.of[Try](0, 1)
      _ <- Try { Bounds.BoundsOps(a) > 0 shouldEqual false }
    } yield {}
    result shouldEqual ().pure[Try]
  }

  test("Bounds(0, 1) > 1") {
    val result = for {
      a <- Bounds.of[Try](0, 1)
      _ <- Try { Bounds.BoundsOps(a) > 1 shouldEqual false }
    } yield {}
    result shouldEqual ().pure[Try]
  }

  test("Bounds(1) > 0") {
    Bounds.BoundsOps(Bounds(1)) > 0 shouldEqual true
  }

  test("Bounds(1, 2) > 0") {
    val result = for {
      a <- Bounds.of[Try](1, 2)
      _ <- Try { Bounds.BoundsOps(a) > 0 shouldEqual true }
    } yield {}
    result shouldEqual ().pure[Try]
  }

  test("Bounds(0) < 0") {
    Bounds.BoundsOps(Bounds(0)) < 0 shouldEqual false
  }

  test("Bounds(0, 1) < 0") {
    val result = for {
      a <- Bounds.of[Try](0, 1)
      _ <- Try { Bounds.BoundsOps(a) < 0 shouldEqual false }
    } yield {}
    result shouldEqual ().pure[Try]
  }

  test("Bounds(0, 1) < 1") {
    val result = for {
      a <- Bounds.of[Try](0, 1)
      _ <- Try { Bounds.BoundsOps(a) < 1 shouldEqual false }
    } yield {}
    result shouldEqual ().pure[Try]
  }

  test("Bounds(0) < 1") {
    Bounds.BoundsOps(Bounds(0)) < 1 shouldEqual true
  }

  test("Bounds(0, 1) < 2") {
    val result = for {
      a <- Bounds.of[Try](0, 1)
      _ <- Try { Bounds.BoundsOps(a) < 2 shouldEqual true }
    } yield {}
    result shouldEqual ().pure[Try]
  }

  test("Bounds(0) contains 0") {
    Bounds(0).contains(0) shouldEqual true
  }

  test("Bounds(0) contains 1") {
    Bounds(0).contains(1) shouldEqual false
  }

  test("Bounds(0, 1) contains 0") {
    val result = for {
      a <- Bounds.of[Try](0, 1)
      _ <- Try { a contains 0 shouldEqual true }
    } yield {}
    result shouldEqual ().pure[Try]
  }

  test("Bounds(0, 1) contains 1") {
    val result = for {
      a <- Bounds.of[Try](0, 1)
      _ <- Try { a contains 1 shouldEqual true }
    } yield {}
    result shouldEqual ().pure[Try]
  }

  test("Bounds(0, 1) contains 2") {
    val result = for {
      a <- Bounds.of[Try](0, 1)
      _ <- Try { a contains 2 shouldEqual false }
    } yield {}
    result shouldEqual ().pure[Try]
  }

  test("Bounds(1, 2) contains 0") {
    val result = for {
      a <- Bounds.of[Try](1, 2)
      _ <- Try { a contains 0 shouldEqual false }
    } yield {}
    result shouldEqual ().pure[Try]
  }
}
