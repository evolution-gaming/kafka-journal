package com.evolutiongaming.kafka.journal

import cats.Show
import cats.data.{NonEmptyList => Nel}
import org.scalatest.{FunSuite, Matchers}

class PartitionOffsetSpec extends FunSuite with Matchers {

  test("order") {
    val partitionOffsets = Nel.of(
      PartitionOffset(partition = 0, offset = 1),
      PartitionOffset(partition = 1, offset = 0),
      PartitionOffset(partition = 0, offset = 0)
    )
    val expected = Nel.of(
      PartitionOffset(partition = 0, offset = 0),
      PartitionOffset(partition = 0, offset = 1),
      PartitionOffset(partition = 1, offset = 0))
    partitionOffsets.sorted shouldEqual expected
  }

  test("show") {
    val partitionOffset = PartitionOffset(partition = 0, offset = 1)
    Show[PartitionOffset].show(partitionOffset) shouldEqual "0:1"
  }
}
