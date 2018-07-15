package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.kafka.journal.Alias.{Id, SeqNr, SeqNrOps}
import com.evolutiongaming.kafka.journal.FutureHelper._

import scala.concurrent.{ExecutionContext, Future}

object LastSeqNr {

  def apply(
    id: Id,
    from: SeqNr,
    statement: JournalStatement.SelectLastRecord.Type,
    metadata: Metadata)(implicit ec: ExecutionContext): Future[SeqNr] = {

    def apply(last: SeqNr, from: SeqNr, segment: Segment): Future[SeqNr] = {
      for {
        pointer <- statement(id, segment.nr, from)
        seqNr <- pointer.fold(last.future) { pointer =>
          val last = pointer.seqNr
          val from = last.next
          segment.next(from).fold(last.future) { segment =>
            apply(last, from, segment)
          }
        }
      } yield {
        seqNr
      }
    }

    val seqNr = from max metadata.deletedTo
    val fromFixed = seqNr.next
    val segment = Segment(fromFixed, metadata.segmentSize)
    apply(seqNr, fromFixed, segment)
  }
}
