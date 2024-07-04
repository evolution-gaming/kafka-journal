package com.evolutiongaming.kafka.journal

import cats.data.{NonEmptyList => Nel}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CombinationsSpec extends AnyFunSuite with Matchers {

  test("Nil") {
    Combinations(List.empty[Int]) shouldEqual Combinations.Type.empty
  }

  test("List(1)") {
    Combinations(List(1)) shouldEqual List(List(Nel.of(1)))
  }

  test("List(1, 2)") {
    Combinations(List(1, 2)) shouldEqual List(List(Nel.of(1, 2)), List(Nel.of(1), Nel.of(2)))
  }

  test("List(1, 2, 3)") {
    Combinations(List(1, 2, 3)) shouldEqual List(
      List(Nel.of(1, 2, 3)),
      List(Nel.of(1), Nel.of(2, 3)),
      List(Nel.of(1), Nel.of(2), Nel.of(3)),
      List(Nel.of(1, 2), Nel.of(3)),
    )
  }

  test("List(1, 2, 3, 4)") {
    Combinations(List(1, 2, 3, 4)) shouldEqual List(
      List(Nel.of(1, 2, 3, 4)),
      List(Nel.of(1), Nel.of(2, 3, 4)),
      List(Nel.of(1), Nel.of(2), Nel.of(3, 4)),
      List(Nel.of(1, 2), Nel.of(3, 4)),
      List(Nel.of(1), Nel.of(2), Nel.of(3), Nel.of(4)),
      List(Nel.of(1, 2), Nel.of(3), Nel.of(4)),
      List(Nel.of(1), Nel.of(2, 3), Nel.of(4)),
      List(Nel.of(1, 2, 3), Nel.of(4)),
    )
  }
}
