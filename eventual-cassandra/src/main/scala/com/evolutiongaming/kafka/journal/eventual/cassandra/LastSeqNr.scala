package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.kafka.journal.Implicits._
import com.evolutiongaming.kafka.journal.eventual.Pointer
import com.evolutiongaming.kafka.journal.{IO, Key, SeqNr}

object LastSeqNr {

  def apply[F[_] : IO](
    key: Key,
    from: SeqNr,
    metadata: Metadata,
    statement: (Key, SegmentNr, SeqNr) => F[Option[Pointer]]): F[Option[SeqNr]] = {

    def apply(from: SeqNr, last: Option[SeqNr]) = {

      def apply(from: SeqNr, last: Option[SeqNr], segment: Segment): F[Option[SeqNr]] = {
        for {
          pointer <- statement(key, segment.nr, from)
          seqNr <- pointer.fold(last.pure) { pointer =>
            val last = pointer.seqNr
            val result = for {
              from <- last.next
              segment <- segment.next(from)
            } yield {
              apply(from, last.some, segment)
            }
            result getOrElse last.some.pure
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
          case Some(from) => apply(from, deleteTo.some)
          case None       => SeqNr.Max.some.pure
        }
    }
  }
}