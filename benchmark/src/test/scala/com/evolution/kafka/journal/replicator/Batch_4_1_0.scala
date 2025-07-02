package com.evolution.kafka.journal.replicator

import cats.data.NonEmptyList
import cats.syntax.all.*
import com.evolution.kafka.journal.*
import com.evolutiongaming.skafka.Offset

/**
 * Original implementation as it was in version `4.1.0`
 */
private[journal] sealed abstract class Batch_4_1_0 extends Product {

  def offset: Offset
}

private[journal] object Batch_4_1_0 {

  def of(records: NonEmptyList[ActionRecord[Action]]): List[Batch_4_1_0] = {

    // returns `true` when we can optimize Cassandra usage by NOT inserting "previous" Append actions in DB
    // because "current" Delete action will discard them all
    def cut(appends: Appends, delete: Action.Delete): Boolean = {
      val append = appends.records.head.action
      append.range.to <= delete.to.value
    }

    def dropDeleted(appends: Appends, delete: Action.Delete): Option[Appends] = {
      // we cannot drop individual events in record as we do not parse the payload here (`records.head.action.payload: ByteVector`)
      val validRecords = appends.records.collect { case record if record.action.range.to >= delete.to.value => record }
      NonEmptyList.fromList(validRecords).map { Appends(appends.offset, _) }
    }

    records
      .foldLeft(List.empty[Batch_4_1_0]) { (bs, record) =>
        val offset = record.partitionOffset.offset

        def appendsOf(records: NonEmptyList[ActionRecord[Action.Append]]): Appends = {
          Appends(offset, records)
        }

        def deleteOf(to: DeleteTo, origin: Option[Origin], version: Option[Version]): Delete = {
          Delete(offset, to, origin, version)
        }

        def purgeOf(origin: Option[Origin], version: Option[Version]): Purge = {
          Purge(offset, origin, version)
        }

        def actionRecord[A <: Action](a: A): ActionRecord[A] = {
          record.copy(action = a)
        }

        def origin: Option[Origin] = {
          bs.foldRight(none[Origin]) { (b, origin) =>
            origin orElse {
              b match {
                case b: Batch_4_1_0.Delete => b.origin
                case _: Batch_4_1_0.Appends => none
                case b: Batch_4_1_0.Purge => b.origin
              }
            }
          }
        }

        def version: Option[Version] = {
          bs.foldRight(none[Version]) { (b, version) =>
            version orElse {
              b match {
                case b: Batch_4_1_0.Delete => b.version
                case _: Batch_4_1_0.Appends => none
                case b: Batch_4_1_0.Purge => b.version
              }
            }
          }
        }

        bs match {
          case b :: tail =>
            (b, record.action) match {
              case (b: Appends, a: Action.Append) =>
                val records = actionRecord(a) :: b.records
                appendsOf(records) :: tail

              case (b: Appends, _: Action.Mark) =>
                appendsOf(b.records) :: tail

              case (b: Appends, a: Action.Delete) =>
                if (cut(b, a)) {
                  val delete = deleteOf(a.to, origin orElse a.origin, version orElse a.version)
                  // preserve last `append` as we want to update `expireAfter` too, but we do not have [easy] access to it
                  delete :: b.copy(records = NonEmptyList.of(b.records.head)) :: Nil
                } else {
                  val delete = deleteOf(a.to, a.origin, a.version)
                  delete :: bs
                }

              case (_: Appends, a: Action.Purge) =>
                purgeOf(a.origin, a.version) :: Nil

              case (b: Delete, a: Action.Append) =>
                appendsOf(NonEmptyList.of(actionRecord(a))) :: b :: tail

              case (b: Delete, _: Action.Mark) =>
                b.copy(offset = offset) :: tail

              case (b: Delete, a: Action.Delete) =>
                if (a.to > b.to) {
                  val cleanTail: List[Batch_4_1_0] = tail.mapFilter {
                    case appends: Appends => dropDeleted(appends, a)
                    case delete: Delete => delete.some
                    case purge: Purge => purge.some
                  }

                  val delete = deleteOf(a.to, b.origin orElse a.origin, b.version orElse a.version)
                  delete :: cleanTail
                } else {
                  val delete =
                    b.copy(offset = offset, origin = b.origin orElse a.origin, version = b.version orElse a.version)
                  delete :: tail
                }

              case (_: Delete, a: Action.Purge) =>
                purgeOf(a.origin, a.version) :: Nil

              case (b: Purge, a: Action.Append) =>
                appendsOf(NonEmptyList.of(actionRecord(a))) :: b :: Nil

              case (b: Purge, _: Action.Mark) =>
                b.copy(offset = offset) :: Nil

              case (b: Purge, a: Action.Delete) =>
                deleteOf(a.to, a.origin, a.version) :: b :: Nil

              case (_: Purge, a: Action.Purge) =>
                purgeOf(a.origin, a.version) :: Nil
            }

          case Nil =>
            record.action match {
              case a: Action.Append => appendsOf(NonEmptyList.of(actionRecord(a))) :: Nil
              case _: Action.Mark => Nil
              case a: Action.Delete => deleteOf(a.to, a.origin, a.version) :: Nil
              case a: Action.Purge => purgeOf(a.origin, a.version) :: Nil
            }
        }
      }
      .foldLeft(List.empty[Batch_4_1_0]) { (bs, b) => // reverse order of Batch_4_1_0es
        b match {
          case b: Appends => b.copy(records = b.records.reverse) :: bs // reverse append actions
          case b: Delete => b :: bs
          case b: Purge => b :: bs
        }
      }
  }

  final case class Appends(
    offset: Offset,
    records: NonEmptyList[ActionRecord[Action.Append]],
  ) extends Batch_4_1_0

  final case class Delete(
    offset: Offset,
    to: DeleteTo,
    origin: Option[Origin],
    version: Option[Version],
  ) extends Batch_4_1_0

  final case class Purge(
    offset: Offset,
    origin: Option[Origin],
    version: Option[Version],
  ) extends Batch_4_1_0
}
