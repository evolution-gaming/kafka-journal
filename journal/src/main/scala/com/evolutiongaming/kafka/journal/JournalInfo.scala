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
      case a: Delete => Deleted(a.to)
      case _: Mark   => this
    }
  }


  final case class NonEmpty(lastSeqNr: SeqNr, deleteTo: Option[SeqNr] = None) extends JournalInfo {

    def apply(a: Action.Header): JournalInfo = {

      def onDelete(a: Delete) = {
        val deleteTo = this.deleteTo.fold(a.to) { _ max a.to } min lastSeqNr
        NonEmpty(lastSeqNr, Some(deleteTo))
      }

      def onAppend(a: Append) = {
        val lastSeqNr = a.range.to
        NonEmpty(lastSeqNr, deleteTo)
      }

      a match {
        case a: Append => onAppend(a)
        case a: Delete => onDelete(a)
        case _: Mark   => this
      }
    }
  }


  final case class Deleted(deleteTo: SeqNr) extends JournalInfo { self =>

    def apply(a: Action.Header): JournalInfo = {

      def onAppend(a: Append) = {
        val deleteTo = a.range.from.prev.map(_ min self.deleteTo)
        NonEmpty(a.range.to, deleteTo)
      }

      a match {
        case a: Append => onAppend(a)
        case a: Delete => Deleted(deleteTo max a.to)
        case _: Mark   => this
      }
    }
  }
}

