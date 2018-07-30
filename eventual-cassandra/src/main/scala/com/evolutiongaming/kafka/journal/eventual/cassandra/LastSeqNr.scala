package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.eventual.cassandra.Helper._
import com.evolutiongaming.kafka.journal.{Key, SeqNr}

object LastSeqNr {

  // TODO test this
  def apply(
    key: Key,
    from: SeqNr,
    metadata: Metadata,
    statement: JournalStatement.SelectLastRecord.Type): Async[Option[SeqNr]] = {

    def apply(from: SeqNr, last: Option[SeqNr]) = {

      def apply(from: SeqNr, last: Option[SeqNr], segment: Segment): Async[Option[SeqNr]] = {
        for {
          pointer <- statement(key, segment.nr, from)
          seqNr <- pointer.fold(last.async) { pointer =>
            val last = pointer.seqNr
            val result = for {
              from <- last.nextOpt
              segment <- segment.next(from)
            } yield {
              apply(from, Some(last), segment)
            }
            result getOrElse Some(last).async
          }
        } yield seqNr
      }

      val segment = Segment(from, metadata.segmentSize)
      apply(from, last, segment)
    }

    metadata.deleteTo.fold(apply(from, None)) { deleteTo =>
      if (from > deleteTo) apply(from, None)
      else deleteTo.nextOpt.fold(SeqNr.Max.some.async) { from =>
        apply(from, Some(deleteTo))
      }
    }
  }
}
