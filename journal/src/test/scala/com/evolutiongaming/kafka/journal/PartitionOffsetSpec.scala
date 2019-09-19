package com.evolutiongaming.kafka.journal

import cats.data.{NonEmptyList => Nel}
import cats.implicits._
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
    partitionOffset.show shouldEqual "0:1"
  }
}
