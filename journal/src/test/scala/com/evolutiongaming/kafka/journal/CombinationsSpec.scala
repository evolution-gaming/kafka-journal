package com.evolutiongaming.kafka.journal

import com.evolutiongaming.nel.Nel
import org.scalatest.{FunSuite, Matchers}

class CombinationsSpec extends FunSuite with Matchers {

  test("Nil") {
    Combinations(List.empty[Int]) shouldEqual Combinations.Type.empty
  }

  test("List(1)") {
    Combinations(List(1)) shouldEqual List(List(Nel(1)))
  }

  test("List(1, 2)") {
    Combinations(List(1, 2)) shouldEqual List(
      List(Nel(1, 2)),
      List(Nel(1), Nel(2)))
  }

  test("List(1, 2, 3)") {
    Combinations(List(1, 2, 3)) shouldEqual List(
      List(Nel(1, 2, 3)),
      List(Nel(1), Nel(2, 3)),
      List(Nel(1), Nel(2), Nel(3)),
      List(Nel(1, 2), Nel(3)))
  }

  test("List(1, 2, 3, 4)") {
    Combinations(List(1, 2, 3, 4)) shouldEqual List(
      List(Nel(1, 2, 3, 4)),
      List(Nel(1), Nel(2, 3, 4)),
      List(Nel(1), Nel(2), Nel(3, 4)),
      List(Nel(1, 2), Nel(3, 4)),
      List(Nel(1), Nel(2), Nel(3), Nel(4)),
      List(Nel(1, 2), Nel(3), Nel(4)),
      List(Nel(1), Nel(2, 3), Nel(4)),
      List(Nel(1, 2, 3), Nel(4)))
  }
}
