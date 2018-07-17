package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.Alias.{Id, SeqNr, SeqNrOps}

object LastSeqNr {

  def apply(
    id: Id,
    from: SeqNr,
    statement: JournalStatement.SelectLastRecord.Type,
    metadata: Metadata): Async[SeqNr] = {

    def apply(last: SeqNr, from: SeqNr, segment: Segment): Async[SeqNr] = {
      for {
        pointer <- statement(id, segment.nr, from)
        seqNr <- pointer.fold(last.async) { pointer =>
          val last = pointer.seqNr
          val from = last.next
          segment.next(from).fold(last.async) { segment =>
            apply(last, from, segment)
          }
        }
      } yield {
        seqNr
      }
    }

    val seqNr = from max metadata.deleteTo
    val fromFixed = seqNr.next
    val segment = Segment(fromFixed, metadata.segmentSize)
    apply(seqNr, fromFixed, segment)
  }
}
