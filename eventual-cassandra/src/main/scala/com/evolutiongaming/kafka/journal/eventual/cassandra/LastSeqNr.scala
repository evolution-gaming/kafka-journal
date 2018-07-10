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

    val segmentSize = metadata.segmentSize

    def segmentOf(seqNr: SeqNr) = Segment(seqNr, segmentSize)

    def lastSeqNr(from: SeqNr): Future[SeqNr] = {
      val seqNrNext = from.next
      val segment = segmentOf(seqNrNext)

      val result = for {
        records <- statement(id, segment, from)
      } yield {

        val x = records.map { _.seqNr }

        x match {
          case None => from.future

          case Some(seqNr) =>
            val segmentNext = segmentOf(seqNrNext)
            if (segment != segmentNext) {
              lastSeqNr(seqNr)
            } else {
              seqNr.future
            }
        }
      }

      result.flatten
    }

    val deletedTo = metadata.deletedTo

    val from2 = deletedTo max from
    lastSeqNr(from2)
  }
}
