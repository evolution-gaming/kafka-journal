package com.evolutiongaming.kafka.journal.eventual

import com.evolutiongaming.skafka.{Offset, Partition}
import org.scalatest.matchers.should.Matchers
import org.scalatest.funsuite.AnyFunSuite

class TopicPointersTest extends AnyFunSuite with Matchers {

  test("merge") {

    def topicPointers(values: Map[Int, Int]) = {
      val values1 = values.map { case (partition, offset) =>
        (Partition.unsafe(partition), Offset.unsafe(offset))
      }
      TopicPointers(values1)
    }

    val topicPointers0 = topicPointers(Map((0, 0), (1, 1), (2, 2)))
    val topicPointers1 = topicPointers(Map((0, 2), (1, 1), (2, 0)))
    val expected = topicPointers(Map((0, 2), (1, 1), (2, 2)))
    (topicPointers0 merge topicPointers1) shouldEqual expected
  }
}
