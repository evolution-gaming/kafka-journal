package com.evolution.kafka.journal

import cats.data.NonEmptyList as Nel
import cats.syntax.all.*
import com.evolutiongaming.skafka.{Offset, Partition}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PartitionOffsetSpec extends AnyFunSuite with Matchers {

  def partitionOffsetOf(partition: Int, offset: Int): PartitionOffset = {
    PartitionOffset(partition = Partition.unsafe(partition), offset = Offset.unsafe(offset))
  }

  test("order") {

    val partitionOffsets = Nel.of(
      partitionOffsetOf(partition = 0, offset = 1),
      partitionOffsetOf(partition = 1, offset = 0),
      partitionOffsetOf(partition = 0, offset = 0),
    )
    val expected = Nel.of(
      partitionOffsetOf(partition = 0, offset = 0),
      partitionOffsetOf(partition = 0, offset = 1),
      partitionOffsetOf(partition = 1, offset = 0),
    )
    partitionOffsets.sorted shouldEqual expected
  }

  test("show") {
    val partitionOffset = partitionOffsetOf(partition = 0, offset = 1)
    partitionOffset.show shouldEqual "0:1"
  }
}
