package com.evolution.kafka.journal.replicator

import cats.data.NonEmptySet as Nes
import cats.syntax.all.*
import com.evolution.kafka.journal.eventual.cassandra.{SegmentNr, Segments}
import com.evolutiongaming.catshelper.DataHelper.*
import com.evolutiongaming.skafka.Partition
import org.scalatest.Succeeded
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.SortedSet
import scala.util.Try

class PartitionsToSegmentsTest extends AnyFunSuite with Matchers {
  for {
    (partitions, partitionNrs, segmentNrs) <- List(
      (20, Nes.of(0), SortedSet(0, 20, 40, 60, 80)),
      (30, Nes.of(0), SortedSet(0, 30, 60, 90)),
      (100, Nes.of(0, 1, 2), SortedSet(0, 1, 2)),
      (1, Nes.of(0), (0 until 100).toSortedSet),
    )
  } {
    test(s"partitions: $partitions, segmentNrs: $segmentNrs, partitionNrs: $partitionNrs") {
      val result = for {
        segmentNrs <- segmentNrs.toList.traverse { a => SegmentNr.of[Try](a.toLong) }
        partitionNrs <- partitionNrs.toNel.traverse { a => Partition.of[Try](a) }
      } yield {
        val partitionsToSegments = PartitionsToSegments(partitions, Segments.old)
        val actual = partitionsToSegments(partitionNrs.toNes)
        actual shouldEqual segmentNrs.toSortedSet
      }
      result shouldEqual Succeeded.pure[Try]
    }
  }
}
