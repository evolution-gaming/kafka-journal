package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.kafka.journal.IO.Implicits._
import com.evolutiongaming.kafka.journal._

object LastSeqNr {

  def apply[F[_] : IO](
    key: Key,
    from: SeqNr,
    metadata: Metadata,
    lastRecord: JournalStatement.SelectLastRecord.Type[F],
    log: Log[F]): F[Option[Pointer]] = {

    def apply(from: SeqNr, last: Option[Pointer]) = {

      def apply(from: SeqNr, last: Option[Pointer], segment: Segment): F[Option[Pointer]] = {
        for {
          pointer <- lastRecord(key, segment.nr, from)
          _ <- log.debug(s"$key lastSeqNr, pointer: $pointer, from: $from, last: $last, segment: $segment") // TODO temporary
          seqNr <- pointer.fold(last.pure) { pointer =>
            val result = for {
              from <- pointer.seqNr.next
              segment <- segment.next(from)
            } yield {
              apply(from, pointer.some, segment)
            }
            result getOrElse pointer.some.pure
          }
        } yield seqNr
      }

      val segment = Segment(from, metadata.segmentSize) // TODO throws exception
      apply(from, last, segment)
    }

    metadata.deleteTo match {
      case None           => apply(from, none)
      case Some(deleteTo) =>
        if (from > deleteTo) apply(from, none)
        else deleteTo.next match {
          case Some(from) => apply(from, Pointer(metadata.partitionOffset, deleteTo).some).map(_.map(_.copy(partitionOffset = metadata.partitionOffset))) // TODO
          case None       => Pointer(metadata.partitionOffset, SeqNr.Max).some.pure
        }
    }
  }
}