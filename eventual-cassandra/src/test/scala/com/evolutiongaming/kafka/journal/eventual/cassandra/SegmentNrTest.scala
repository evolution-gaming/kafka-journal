package com.evolutiongaming.kafka.journal.eventual.cassandra

import org.scalatest.matchers.should.Matchers

import scala.util.Try
import org.scalatest.funsuite.AnyFunSuite

class SegmentNrTest extends AnyFunSuite with Matchers {

  test("next") {
    val result = for {
      a <- SegmentNr.min.next[Try]
      b <- SegmentNr.of[Try](SegmentNr.min.value + 1)
      _  = a shouldEqual b
    } yield {}
    result.get
  }

  test("prev") {
    val result = for {
      a <- SegmentNr.max.prev[Try]
      b <- SegmentNr.of[Try](SegmentNr.max.value - 1)
      _  = a shouldEqual b
    } yield {}
    result.get
  }

  List(
    (2, 0, List.empty[Int]),
    (0, 2, List(0, 1, 2)),
    (0, 0, List(0)),
  ).foreach { case (a, b, expected) =>
    test(s"$a to $b") {
      val result = for {
        a <- SegmentNr.of[Try](a.toLong)
        b <- SegmentNr.of[Try](b.toLong)
        c <- a.to[Try](b)
        _  = c.map { _.value } shouldEqual expected.map { _.toLong }
      } yield {}
      result.get
    }
  }
}
