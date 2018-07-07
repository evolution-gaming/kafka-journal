package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.kafka.journal.Alias.{Id, SeqNr}
import com.evolutiongaming.kafka.journal.Alias.SeqNrOps

import scala.concurrent.{ExecutionContext, Future}

object LastSeqNr {

  def apply(
    id: Id,
    from: SeqNr,
    statement: JournalStatement.SelectLastRecord.Type,
    metadata: Metadata)(implicit ec: ExecutionContext): Future[SeqNr] = {

    println(s"$id EventualCassandra.list metadata: $metadata")

    val segmentSize = metadata.segmentSize

    def segmentOf(seqNr: SeqNr) = Segment(seqNr, segmentSize)

    def lastSeqNr(seqNr: SeqNr): Future[SeqNr] = {
      val seqNrNext = seqNr.next
      val segment = segmentOf(seqNrNext)

      val result = for {
        records <- statement(id, segment, seqNr)
      } yield {

        val x = records.map{_.seqNr}

        x match {
          case None => Future.successful(seqNr)

          case Some(seqNr) =>
            val segmentNext = segmentOf(seqNrNext)
            if(segment != segmentNext) {
              lastSeqNr(seqNr)
            } else {
              Future.successful(seqNr)
            }
        }
      }

      result.flatten
    }

    val deletedTo = metadata.deletedTo

    println(s"$id EventualCassandra.list deletedTo: $deletedTo")

    val from2 = deletedTo max from
    lastSeqNr(from2)
  }
}
