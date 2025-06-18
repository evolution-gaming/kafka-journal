package org.apache.pekko.persistence.kafka.journal

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class GroupByWeightSpec extends AnyFunSuite with Matchers {

  for {
    (weight, value, expected) <- List(
      (10, Nil, Nil),
      (-1, List(1), List(List(1))),
      (1, List(1, 2, 3), List(List(1), List(2), List(3))),
      (3, List(1, 2, 3, 4), List(List(1, 2), List(3), List(4))),
      (7, List(1, 2, 3, 4), List(List(1, 2, 3), List(4))),
      (4, List(1, 3, 1, 2), List(List(1, 3), List(1, 2))),
    )
  } {
    test(s"group value: $value, weight: $weight") {
      GroupByWeight(value, weight)(identity) shouldEqual expected
    }
  }
}
