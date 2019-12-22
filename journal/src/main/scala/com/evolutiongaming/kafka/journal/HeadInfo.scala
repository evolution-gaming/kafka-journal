package com.evolutiongaming.kafka.journal

import cats.implicits._
import cats.{Foldable, Semigroup}
import com.evolutiongaming.skafka.Offset


sealed abstract class HeadInfo extends Product

object HeadInfo {

  def empty: HeadInfo = Empty

  def delete(deleteTo: SeqNr): HeadInfo = Delete(deleteTo)

  def append(seqNr: SeqNr, deleteTo: Option[SeqNr], offset: Offset): HeadInfo = {
    Append(offset, seqNr, deleteTo)
  }

  def apply[F[_] : Foldable](actions: F[(ActionHeader, Offset)]): HeadInfo = {
    actions.foldLeft(empty) { case (head, (header, offset)) => head(header, offset) }
  }


  final case object Empty extends HeadInfo


  abstract sealed class NonEmpty extends HeadInfo

  object NonEmpty {

    implicit val semigroupNonEmpty: Semigroup[NonEmpty] = {
      (a: NonEmpty, b: NonEmpty) => {

        def onDelete(a: Append, b: Delete) = {
          val deleteTo = a.deleteTo.fold(b.deleteTo) { _ max b.deleteTo }
          Append(a.offset, a.seqNr, deleteTo.some)
        }

        (a, b) match {
          case (a: Append, b: Append) => Append(a.offset min b.offset, a.seqNr max b.seqNr, a.deleteTo max b.deleteTo)
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


  final case class Append(
    offset: Offset,
    seqNr: SeqNr,
    deleteTo: Option[SeqNr]
  ) extends NonEmpty


  final case class Delete(
    deleteTo: SeqNr
  ) extends NonEmpty


  final case object Purge extends NonEmpty


  implicit class HeadInfoOps(val self: HeadInfo) extends AnyVal {

    def delete(to: SeqNr): HeadInfo = {

      def onAppend(self: Append) = {
        val deleteTo = self
          .deleteTo
          .fold(to) { _ max to }
          .min(self.seqNr)
        self.copy(seqNr = self.seqNr, deleteTo = deleteTo.some)
      }

      def delete = Delete(to)

      self match {
        case a: Append => onAppend(a)
        case a: Delete => Delete(a.deleteTo max to)
        case Empty     => delete
        case Purge     => delete
      }
    }


    def append(range: SeqRange, offset: Offset): HeadInfo = {

      def deleteToOf(deleteTo: SeqNr) = range
        .from
        .prev[Option]
        .map { _ min deleteTo }

      def append = Append(offset, range.to, none)

      self match {
        case a: Append => a.copy(seqNr = range.to)
        case a: Delete => Append(offset, range.to, deleteToOf(a.deleteTo))
        case Empty     => append
        case Purge     => append
      }
    }


    def mark: HeadInfo = self


    def purge: HeadInfo = Purge


    def apply(header: ActionHeader, offset: Offset): HeadInfo = {
      header match {
        case a: ActionHeader.Append => append(a.range, offset)
        case _: ActionHeader.Mark   => mark
        case a: ActionHeader.Delete => delete(a.to)
        case _: ActionHeader.Purge  => purge
      }
    }
  }
}

