package com.evolutiongaming.kafka.journal

import cats.implicits._
import cats.{Foldable, Semigroup}

import com.evolutiongaming.kafka.journal.util.OptionHelper._


sealed abstract class HeadInfo extends Product {

  def apply(header: ActionHeader): HeadInfo
}

object HeadInfo {

  def empty: HeadInfo = Empty

  def delete(deleteTo: SeqNr): HeadInfo = Delete(deleteTo)

  def append(seqNr: SeqNr, deleteTo: Option[SeqNr] = None): HeadInfo = Append(seqNr, deleteTo)

  def apply[T[_] : Foldable](actions: T[ActionHeader]): HeadInfo = {
    Foldable[T].foldLeft(actions, HeadInfo.empty)(_ apply _)
  }


  final case object Empty extends HeadInfo { self =>

    def apply(a: ActionHeader) = a match {
      case a: ActionHeader.Append => Append(a.range.to, None)
      case a: ActionHeader.Delete => Delete(a.to)
      case _: ActionHeader.Mark   => self
    }
  }


  abstract sealed class NonEmpty extends HeadInfo

  object NonEmpty {

    implicit val semigroupNonEmpty: Semigroup[NonEmpty] = new Semigroup[NonEmpty] {

      def combine(x: NonEmpty, y: NonEmpty) = {

        def onDelete(x: Append, y: Delete) = {
          val deleteTo = x.deleteTo.fold(y.deleteTo)(_ max y.deleteTo)
          Append(x.seqNr, deleteTo.some)
        }

        (x, y) match {
          case (x: Append, y: Append) => Append(x.seqNr max y.seqNr, x.deleteTo max y.deleteTo)
          case (x: Append, y: Delete) => onDelete(x, y)
          case (x: Delete, y: Append) => onDelete(y, x)
          case (x: Delete, y: Delete) => Delete(x.deleteTo max y.deleteTo)
        }
      }
    }
  }


  final case class Append(seqNr: SeqNr, deleteTo: Option[SeqNr] = None) extends NonEmpty { self =>

    def apply(a: ActionHeader): NonEmpty = {

      def onDelete(a: ActionHeader.Delete) = {
        val deleteTo = self.deleteTo.fold(a.to) { _ max a.to } min seqNr
        Append(seqNr, deleteTo.some)
      }

      def onAppend(a: ActionHeader.Append) = {
        val lastSeqNr = a.range.to
        Append(lastSeqNr, deleteTo)
      }

      a match {
        case a: ActionHeader.Append => onAppend(a)
        case a: ActionHeader.Delete => onDelete(a)
        case _: ActionHeader.Mark   => self
      }
    }
  }


  final case class Delete(deleteTo: SeqNr) extends NonEmpty { self =>

    def apply(a: ActionHeader): NonEmpty = {

      def onAppend(a: ActionHeader.Append) = {
        val deleteTo = a.range.from.prev[Option].map(_ min self.deleteTo)
        Append(a.range.to, deleteTo)
      }

      a match {
        case a: ActionHeader.Append => onAppend(a)
        case a: ActionHeader.Delete => Delete(deleteTo max a.to)
        case _: ActionHeader.Mark   => self
      }
    }
  }
}

