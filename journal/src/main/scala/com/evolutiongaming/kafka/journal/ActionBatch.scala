package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.Action.Header._
import com.evolutiongaming.kafka.journal.Alias._

sealed trait ActionBatch {
  def apply(a: Action.Header): ActionBatch
}

object ActionBatch {

  def empty: ActionBatch = Empty

  def apply(actions: Iterable[Action.Header]): ActionBatch = {
    actions.foldLeft[ActionBatch](ActionBatch.Empty)(_ apply _)
  }


  final case object Empty extends ActionBatch {

    def apply(a: Action.Header): ActionBatch = a match {
      case a: Append => NonEmpty(a.range.to, None)
      case a: Delete => DeleteTo(a.to)
      case _: Mark   => this
    }
  }


  // TODO consider adding DeleteAll case

  final case class NonEmpty(lastSeqNr: SeqNr, deleteTo: Option[SeqNr] = None) extends ActionBatch {

    require(
      deleteTo.forall(_ > 0),
      s"deleteTo(${ deleteTo.getOrElse(0) }) > 0")

    require(
      deleteTo.forall(_ <= lastSeqNr),
      s"lastSeqNr($lastSeqNr) >= deleteTo(${ deleteTo.getOrElse(0) })")

    def apply(a: Action.Header): ActionBatch = {

      def nonEmpty(a: Delete) = {
        val deleteTo = this.deleteTo.fold(a.to) { _ max a.to } min lastSeqNr
        NonEmpty(lastSeqNr, Some(deleteTo))
      }

      a match {
        case a: Append => NonEmpty(a.range.to, deleteTo)
        case a: Delete => nonEmpty(a)
        case _: Mark   => this
      }
    }
  }


  final case class DeleteTo(seqNr: SeqNr) extends ActionBatch {

    require(seqNr > 0, s"seqNr($seqNr) > 0")

    def apply(a: Action.Header): ActionBatch = {

      def nonEmpty(a: Append) = {
        val deleteTo = {
          val deleteTo = seqNr min a.range.from.prev
          if (deleteTo > 0) Some(deleteTo) else None
        }
        NonEmpty(a.range.to, deleteTo)
      }

      a match {
        case a: Append => nonEmpty(a)
        case a: Delete => DeleteTo(seqNr max a.to)
        case _: Mark   => this
      }
    }
  }
}

