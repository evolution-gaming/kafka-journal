package com.evolutiongaming.kafka.journal.stream

import cats.Id
import org.scalatest.{FunSuite, Matchers}

class StreamSpec extends FunSuite with Matchers {

  test("lift") {
    Stream.lift[Id, Int](0).toList shouldEqual List(0)
  }

  test("single") {
    Stream[Id].single(0).toList shouldEqual List(0)
  }

  test("empty") {
    Stream.empty[Id, Int].toList shouldEqual Nil
  }

  test("map") {
    Stream.lift[Id, Int](0).map(_ + 1).toList shouldEqual List(1)
  }

  test("flatMap") {
    val stream = Stream.lift[Id, Int](1)
    val stream1 = for {
      a <- stream
      b <- stream
    } yield a + b

    stream1.toList shouldEqual List(2)
  }

  test("take") {
    Stream.lift[Id, Int](0).take(3).toList shouldEqual List(0)
    Stream[Id].many(1, 2, 3).take(1).toList shouldEqual List(1)
  }

  test("first") {
    Stream[Id].single(0).first shouldEqual Some(0)
    Stream.empty[Id, Int].first shouldEqual None
  }

  test("last") {
    Stream[Id].many(1, 2, 3).last shouldEqual Some(3)
    Stream.empty[Id, Int].last shouldEqual None
  }

  test("length") {
    Stream.repeat[Id, Int](0).take(3).length shouldEqual 3
  }

  test("repeat") {
    Stream.repeat[Id, Int](0).take(3).toList shouldEqual List.fill(3)(0)
  }

  test("filter") {
    Stream[Id].many(1, 2, 3).filter(_ >= 2).toList shouldEqual List(2, 3)
  }

  test("collect") {
    Stream[Id].many(1, 2, 3).collect { case x if x >= 2 => x + 1 }.toList shouldEqual List(3, 4)
  }

  test("zipWithIndex") {
    Stream.repeat[Id, Int](0).zipWithIndex.take(3).toList shouldEqual List((0, 0), (0, 1), (0, 2))
  }

  test("takeWhile") {
    Stream[Id].many(1, 2, 1).takeWhile(_ < 2).toList shouldEqual List(1)
  }

  test("dropWhile") {
    Stream[Id].many(1, 2, 1).dropWhile(_ < 2).toList shouldEqual List(2, 1)
  }
}
