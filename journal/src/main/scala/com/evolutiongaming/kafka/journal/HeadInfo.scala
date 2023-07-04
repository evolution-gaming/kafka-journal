package com.evolutiongaming.kafka.journal

import cats.syntax.all._
import cats.{Foldable, Semigroup}
import com.evolutiongaming.skafka.Offset

/** Minimal metainformation about the non-replicated events in a journal.
  *
  * If journal has a non-deleted events pending a replication, it will contain
  * [[Offset]] of the _first_ (i.e. oldest) event and [[SeqNr]] of the _last_
  * (i.e. newest) event in a form of [[HeadInfo.Append]] class, with
  * [[HeadInfo.Append#deleteTo]] specifying the part to be deleted, if any.
  *
  * If the journal was purged or all events were deleted, it will be
  * [[HeadInfo.Purge]] or [[HeadInfo.Delete]], accordingly.
  *
  * Having this information (by preparing it using [[HeadCache]]) allows one to
  * avoid re-reading Kafka during recovery, if it is [[HeadInfo.Empty]], or
  * start reading Kafka topic partition much later, by skipping events unrelated
  * to the journal.
  *
  * @see
  *   [[HeadCache]] for more details on how this information is being
  *   prefetched.
  * @see
  *   [[Journals]] for more details on how it is used.
  */
sealed abstract class HeadInfo extends Product

object HeadInfo {

  def empty: HeadInfo = Empty

  def delete(deleteTo: DeleteTo): HeadInfo = Delete(deleteTo)

  def append(offset: Offset, seqNr: SeqNr, deleteTo: Option[DeleteTo]): HeadInfo = {
    Append(offset, seqNr, deleteTo)
  }

  def apply[F[_] : Foldable](actions: F[(ActionHeader, Offset)]): HeadInfo = {
    actions.foldLeft(empty) { case (head, (header, offset)) => head(header, offset) }
  }


  /** There are no non-replicated events in Kafka for specific journal,
    *
    * Having this state means it is safe to assume the journal is fully
    * replicated to a storage (i.e. Cassandra).
    *
    * In other words, one can read it from Cassandra only and not look into
    * Kafka.
    */
  final case object Empty extends HeadInfo

  /** There are new non-replicated events in Kafka for specific journal,
    *
    * Having this state means it is not enough to read Cassandra to get a full
    * journal. One has to read Kafka also.
    */
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
    deleteTo: Option[DeleteTo]
  ) extends NonEmpty


  final case class Delete(
    deleteTo: DeleteTo
  ) extends NonEmpty


  final case object Purge extends NonEmpty


  implicit class HeadInfoOps(val self: HeadInfo) extends AnyVal {

    def delete(to: DeleteTo): HeadInfo = {

      def onAppend(self: Append) = {
        val deleteTo = self
          .deleteTo
          .fold(to) { _ max to }
          .min(self.seqNr.toDeleteTo)
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

      def deleteToOf(deleteTo: DeleteTo) = range
        .from
        .prev[Option]
        .map { _.toDeleteTo min deleteTo }

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

