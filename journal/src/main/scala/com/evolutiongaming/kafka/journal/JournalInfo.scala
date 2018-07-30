package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.Action.Header._

sealed trait JournalInfo {
  def apply(a: Action.Header): JournalInfo
}

object JournalInfo {

  def empty: JournalInfo = Empty

  def apply(actions: Iterable[Action.Header]): JournalInfo = {
    actions.foldLeft[JournalInfo](JournalInfo.Empty)(_ apply _)
  }


  final case object Empty extends JournalInfo {

    def apply(a: Action.Header): JournalInfo = a match {
      case a: Append => NonEmpty(a.range.to, None)
      case a: Delete => DeleteTo(a.to)
      case _: Mark   => this
    }
  }


  // TODO consider adding DeleteAll case

  final case class NonEmpty(lastSeqNr: SeqNr, deleteTo: Option[SeqNr] = None) extends JournalInfo {

    require(
      deleteTo.forall(_ <= lastSeqNr),
      s"lastSeqNr($lastSeqNr) >= deleteTo(${ deleteTo.getOrElse(0) })")

    def apply(a: Action.Header): JournalInfo = {

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


  final case class DeleteTo(seqNr: SeqNr) extends JournalInfo {

    def apply(a: Action.Header): JournalInfo = {

      def nonEmpty(a: Append) = {
        val deleteTo = for {
          prev <- a.range.from.prevOpt
        } yield seqNr min prev
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

