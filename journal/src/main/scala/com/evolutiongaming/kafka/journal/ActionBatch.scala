package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.Action._
import com.evolutiongaming.kafka.journal.Alias._

sealed trait ActionBatch

object ActionBatch {

  def apply(actions: Iterable[Action]): ActionBatch = {

    def onDelete(b: NonEmpty, a: Delete) = {
      val deleteTo = b.deleteTo.fold(a.seqNr) { _ max a.seqNr } min b.lastSeqNr
      NonEmpty(b.lastSeqNr, Some(deleteTo))
    }

    actions.foldLeft[ActionBatch](Empty) {
      case (b: NonEmpty, a: Append) => NonEmpty(a.range.to, b.deleteTo)
      case (b: NonEmpty, a: Delete) => onDelete(b, a)
      case (b: DeleteTo, a: Append) => NonEmpty(a.range.to, Some(b.seqNr min a.range.from.prev))
      case (b: DeleteTo, a: Delete) => DeleteTo(b.seqNr max a.seqNr)
      case (Empty      , a: Append) => NonEmpty(a.range.to, None)
      case (Empty      , a: Delete) => DeleteTo(a.seqNr)
      case (b          , _: Mark)   => b
    }
  }


  case object Empty extends ActionBatch

  case class NonEmpty(lastSeqNr: SeqNr, deleteTo: Option[SeqNr]) extends ActionBatch

  case class DeleteTo(seqNr: SeqNr) extends ActionBatch
}

