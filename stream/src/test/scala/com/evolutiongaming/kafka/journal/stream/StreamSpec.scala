package com.evolutiongaming.kafka.journal.stream

import org.scalatest.{FunSuite, Matchers}

class StreamSpec extends FunSuite with Matchers {

  test("lift") {
    Stream.lift[cats.Id, Int](0).toList shouldEqual List(0)
  }

  test("single") {
    Stream[cats.Id].single(0).toList shouldEqual List(0)
  }

  test("empty") {
    Stream.empty[cats.Id, Int].toList shouldEqual Nil
  }

  test("map") {
    Stream.lift[cats.Id, Int](0).map(_ + 1).toList shouldEqual List(1)
  }

  test("flatMap") {
    val stream = Stream.lift[cats.Id, Int](1)
    val stream1 = for {
      a <- stream
      b <- stream
    } yield a + b

    stream1.toList shouldEqual List(2)
  }

  test("take") {
    Stream.lift[cats.Id, Int](0).take(3).toList shouldEqual List(0)
    Stream[cats.Id].many(1, 2, 3).take(1).toList shouldEqual List(1)
  }

  test("first") {
    Stream[cats.Id].single(0).first shouldEqual Some(0)
    Stream.empty[cats.Id, Int].first shouldEqual None
  }

  test("last") {
    Stream[cats.Id].many(1, 2, 3).last shouldEqual Some(3)
    Stream.empty[cats.Id, Int].last shouldEqual None
  }

  test("repeat") {
    Stream.repeat[cats.Id, Int](0)
      .take(3)
      .toList shouldEqual List.fill(3)(0)
  }

  test("filter") {
    Stream[cats.Id].many(1, 2, 3).filter(_ >= 2).toList shouldEqual List(2, 3)
  }

  test("zipWithIndex") {
    Stream.repeat[cats.Id, Int](0).zipWithIndex.take(3).toList shouldEqual List((0, 0), (0, 1), (0, 2))
  }

  test("takeWhile") {
    Stream[cats.Id].many(1, 2, 1).takeWhile(_ < 2).toList shouldEqual List(1)
  }

  test("dropWhile") {
    Stream[cats.Id].many(1, 2, 1).dropWhile(_ < 2).toList shouldEqual List(2, 1)
  }
}
