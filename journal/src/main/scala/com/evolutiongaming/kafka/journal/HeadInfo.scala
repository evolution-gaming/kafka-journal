package com.evolutiongaming.kafka.journal

import cats.syntax.all.*
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

  /** There are no non-replicated events in Kafka for specific journal.
    *
    * [[HeadInfo#empty]] is equivalent to [[HeadInfo.Empty]], but the inferred
    * type will be `HeadInfo` rather than `HeadInfo.Empty`, which might be a
    * little more convenient in some cases.
    *
    * Example:
    * {{{
    * scala> import com.evolutiongaming.kafka.journal.HeadInfo
    *
    * scala> HeadInfo.Empty
    * val res0: HeadInfo.Empty.type = Empty
    *
    * scala> HeadInfo.empty
    * val res1: HeadInfo = Empty
    * }}}
    *
    * @see [[HeadInfo.Empty]] for more details.
    */
  def empty: HeadInfo = Empty

  /** The only non-replicated records are delete actions.
    *
    * [[HeadInfo#delete]] is equivalent to [[HeadInfo.Delete]], but the inferred
    * type will be `HeadInfo` rather than `HeadInfo.Delete`, which might be a
    * little more convenient in some cases.
    *
    * Example:
    * {{{
    * scala> import com.evolutiongaming.kafka.journal.HeadInfo
    *
    * scala> HeadInfo.Delete(DeleteTo(SeqNr.min))
    * val res1: HeadInfo.Delete = Delete(1)
    *
    * scala> HeadInfo.delete(DeleteTo(SeqNr.min))
    * val res0: HeadInfo = Delete(1)
    * }}}
    *
    * @see [[HeadInfo.Delete]] for more details.
    */
  def delete(deleteTo: DeleteTo): HeadInfo = Delete(deleteTo)

  /** There are new appended events in Kafka, which did not replicate yet.
    *
    * [[HeadInfo#append]] is equivalent to [[HeadInfo.Append]], but the inferred
    * type will be `HeadInfo` rather than `HeadInfo.Append`, which might be a
    * little more convenient in some cases.
    *
    * Example:
    * {{{
    * scala> import com.evolutiongaming.kafka.journal.*
    * scala> import com.evolutiongaming.skafka.Offset
    *
    * scala> HeadInfo.Append(Offset.min, SeqNr.min, None)
    * val res0: HeadInfo.Append = Append(0,1,None)
    *
    * scala> HeadInfo.append(Offset.min, SeqNr.min, None)
    * val res1: HeadInfo = Append(0,1,None)
    * }}}
    *
    * @see [[HeadInfo.Append]] for more details.
    */
  def append(offset: Offset, seqNr: SeqNr, deleteTo: Option[DeleteTo]): HeadInfo = {
    Append(offset, seqNr, deleteTo)
  }

  /** Calls [[HeadInfo#apply]] until all incoming actions are handled */
  def apply[F[_]: Foldable](actions: F[(ActionHeader, Offset)]): HeadInfo = {
    actions.foldLeft(empty) { case (head, (header, offset)) => head(header, offset) }
  }

  /** There are no non-replicated events in Kafka for specific journal.
    *
    * Having this state means it is safe to assume the journal is fully
    * replicated to a storage (i.e. Cassandra).
    *
    * In other words, one can read it from Cassandra only and not look into
    * Kafka.
    */
  final case object Empty extends HeadInfo

  /** There are new non-replicated events in Kafka for specific journal.
    *
    * Having this state means it is not enough to read Cassandra to get a full
    * journal. One has to read Kafka also.
    */
  abstract sealed class NonEmpty extends HeadInfo

  object NonEmpty {

    implicit val semigroupNonEmpty: Semigroup[NonEmpty] = { (a: NonEmpty, b: NonEmpty) =>
      {

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

  /** There are new appended events in Kafka, which did not replicate yet.
    *
    * The fields will be located like following inside of the Kafka topic
    * partition:
    * {{{
    * [offset][deleted events][deleteTo][non-replicated events][seqNr]
    * }}}
    *
    * @param offset
    *   [[Offset]] of the _first_ non-replicated event, which might be,
    *   optionally, deleted.
    * @param seqNr
    *   [[SeqNr]] of the _last_ non-replicated event.
    * @param deleteTo
    *   [[SeqNr]] of the _last_ deleted event, if any.
    */
  final case class Append(
      offset: Offset,
      seqNr: SeqNr,
      deleteTo: Option[DeleteTo],
  ) extends NonEmpty

  /** The only non-replicated records are delete actions.
    *
    * The events itself already replicated to Cassandra, so it should be enough
    * to read them from Cassandra starting from the first non-deleted event,
    * i.e. there is no need to read Kafka in this case.
    *
    * The `deleteTo` field will be located like following inside of the Kafka
    * topic partition:
    * {{{
    * [deleted events][deleteTo][replicated events]
    * }}}
    *
    * @param deleteTo
    *   [[SeqNr]] of the _last_ deleted event.
    */
  final case class Delete(
      deleteTo: DeleteTo,
  ) extends NonEmpty

  /** The last non-replicated record was a journal purge action.
    *
    * It means there is no need to read either Cassandra or Kafka as journal was
    * purged.
    */
  final case object Purge extends NonEmpty

  implicit class HeadInfoOps(val self: HeadInfo) extends AnyVal {

    /** A journal tail was dropped.
      *
      * We update the range of deleted non-replicated events.
      */
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

    /** A new event was appended.
      *
      * We update last saved [[SeqNr]], but ignore [[Offset]] unless this is a
      * first event in our list of non-replicted events.
      */
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

    /** A journal marker was found, which does not change [[HeadInfo]] state.
      *
      * The marker only matters during recovery, to validate that all journal
      * events were read. There is no, currently, a reason to track if markers
      * made by previous recoveries did replicate.
      */
    def mark: HeadInfo = self

    /** A journal purge was performed, all previous events could be ignored */
    def purge: HeadInfo = Purge

    /** Update [[HeadInfo]] with a next event coming from Kafka.
      *
      * Note, that no actual event body is required, only [[ActionHeader]] and
      * [[Offset]] of the new record.
      */
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
