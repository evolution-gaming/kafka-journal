package com.evolutiongaming.kafka.journal.replicator

import cats.Monad
import cats.data.{NonEmptySet => Nes}
import cats.syntax.all._
import com.evolutiongaming.catshelper.DataHelper._
import com.evolutiongaming.kafka.journal.eventual.cassandra.{SegmentNr, Segments}
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.skafka.Partition

import scala.collection.immutable.SortedSet


trait PartitionsToSegments {

  def apply(partitions: Nes[Partition]): SortedSet[SegmentNr]
}

object PartitionsToSegments {

  def of[F[_]: Monad: Fail](
    partitions: Int,
    segments: Segments = Segments.old
  ): F[PartitionsToSegments] = {

    (SegmentNr.min.value until segments.value.toLong)
      .toList
      .traverse { segment => SegmentNr.of[F](segment) }
      .map { segmentNrs =>
        val filter = {
          if (partitions >= segments.value) {
            (a: Partition, b: SegmentNr) => a.value % segments.value.toLong === b.value
          } else {
            (a: Partition, b: SegmentNr) => b.value % partitions === a.value.toLong
          }
        }

        partitions: Nes[Partition] => {
          for {
            partition <- partitions.toSortedSet
            segmentNr <- segmentNrs.toSortedSet
            if filter(partition, segmentNr)
          } yield segmentNr
        }
      }
  }
}
