package com.evolutiongaming.kafka.journal

import cats.implicits._
import cats.{Foldable, Semigroup}
import com.evolutiongaming.kafka.journal.util.OptionHelper._


sealed abstract class HeadInfo extends Product

object HeadInfo {

  def empty: HeadInfo = Empty

  def delete(deleteTo: SeqNr): HeadInfo = Delete(deleteTo)

  def append(seqNr: SeqNr, deleteTo: Option[SeqNr]): HeadInfo = Append(seqNr, deleteTo)

  def apply[F[_] : Foldable](actions: F[ActionHeader]): HeadInfo = {
    actions.foldLeft(HeadInfo.empty) { _ apply _ }
  }


  final case object Empty extends HeadInfo


  abstract sealed class NonEmpty extends HeadInfo

  object NonEmpty {

    implicit val semigroupNonEmpty: Semigroup[NonEmpty] = {
      (x: NonEmpty, y: NonEmpty) => {

        def onDelete(x: Append, y: Delete) = {
          val deleteTo = x.deleteTo.fold(y.deleteTo) { _ max y.deleteTo }
          Append(seqNr = x.seqNr, deleteTo = deleteTo.some)
        }

        (x, y) match {
          case (a: Append, b: Append) => Append(a.seqNr max b.seqNr, a.deleteTo max b.deleteTo)
          case (a: Append, b: Delete) => onDelete(a, b)
          case (_: Append, Purge)     => Purge
          case (a: Delete, b: Append) => onDelete(b, a)
          case (a: Delete, b: Delete) => Delete(a.deleteTo max b.deleteTo)
          case (_: Delete, Purge)     => Purge
          case (Purge, b: Append)     => b
          case (Purge, b: Delete)     => b
          case (Purge, Purge)         => Purge
        }
      }
    }
  }


  final case class Append(seqNr: SeqNr, deleteTo: Option[SeqNr] = None) extends NonEmpty


  final case class Delete(deleteTo: SeqNr) extends NonEmpty


  final case object Purge extends NonEmpty


  implicit class HeadInfoOps(val self: HeadInfo) extends AnyVal {

    def delete(to: SeqNr): HeadInfo = {

      def onAppend(append: Append) = {
        val deleteTo = append
          .deleteTo
          .fold(to) { _ max to }
          .min(append.seqNr)
        Append(
          seqNr = append.seqNr,
          deleteTo = deleteTo.some)
      }

      self match {
        case a: Append => onAppend(a)
        case a: Delete => Delete(a.deleteTo max to)
        case Empty     => Delete(to)
        case Purge     => Delete(to)
      }
    }


    def append(range: SeqRange): HeadInfo = {

      def onDelete(deleteTo: SeqNr) = {
        val deleteTo1 = range
          .from
          .prev[Option]
          .map { _ min deleteTo }
        Append(seqNr = range.to, deleteTo = deleteTo1)
      }

      self match {
        case a: Append => a.copy(seqNr = range.to)
        case a: Delete => onDelete(a.deleteTo)
        case Empty     => Append(range.to, none)
        case Purge     => Append(range.to, none)
      }
    }


    def mark: HeadInfo = self


    def purge: HeadInfo = Purge


    def apply(header: ActionHeader): HeadInfo = {
      header match {
        case a: ActionHeader.Append => append(a.range)
        case _: ActionHeader.Mark   => mark
        case a: ActionHeader.Delete => delete(a.to)
        case _: ActionHeader.Purge  => purge
      }
    }
  }
}

