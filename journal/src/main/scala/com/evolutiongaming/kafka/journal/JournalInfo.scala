package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.Action._

sealed trait JournalInfo {
  def apply(action: Action): JournalInfo
}

object JournalInfo {

  def empty: JournalInfo = Empty

  def apply(actions: Iterable[Action]): JournalInfo = {
    actions.foldLeft[JournalInfo](JournalInfo.Empty)(_ apply _)
  }


  final case object Empty extends JournalInfo {

    def apply(a: Action): JournalInfo = a match {
      case a: Append => NonEmpty(a.range.to, None)
      case a: Delete => Deleted(a.to)
      case _: Mark   => this
    }
  }


  final case class NonEmpty(seqNr: SeqNr, deleteTo: Option[SeqNr] = None) extends JournalInfo {

    def apply(a: Action): JournalInfo = {

      def onDelete(a: Delete) = {
        val deleteTo = this.deleteTo.fold(a.to) { _ max a.to } min seqNr
        NonEmpty(seqNr, Some(deleteTo))
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

    def apply(a: Action): JournalInfo = {

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

