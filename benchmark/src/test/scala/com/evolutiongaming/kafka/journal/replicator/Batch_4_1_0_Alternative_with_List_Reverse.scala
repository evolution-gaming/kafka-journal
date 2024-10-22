package com.evolutiongaming.kafka.journal.replicator

import cats.data.NonEmptyList
import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.*
import com.evolutiongaming.skafka.Offset

/**
 * Copy of `Batch` with changes:
 *  - change batching algorithm so it is easier to comprehend
 *  - records are aggregated within `List` using `prepend` and later collection gets reversed  
 */
private[journal] sealed abstract class Batch_4_1_0_Alternative_with_List_Reverse extends Product {

  def offset: Offset
}

private[journal] object Batch_4_1_0_Alternative_with_List_Reverse {

  def of(records: NonEmptyList[ActionRecord[Action]]): List[Batch_4_1_0_Alternative_with_List_Reverse] = {

    val actions = records
      .foldLeft(List.empty[ActionRecord[Action]]) {
        // drop all `Mark` actions
        case (acc, ActionRecord(_: Action.Mark, _)) => acc

        // drop all actions before `Purge`
        case (_, r @ ActionRecord(_: Action.Purge, _)) => List(r)

        // collect `Append` and `Delete` actions
        case (acc, record) => record +: acc // CHANGE: `prepend` instead of `append`
      }
      .groupBy { record =>
        record.action match {
          case _: Action.Mark   => "M" // cannot be - we remove them in previous step
          case _: Action.Purge  => "P"
          case _: Action.Delete => "D"
          case _: Action.Append => "A"
        }
      }

    val purge = actions.get("P").flatMap {
      // can be at most one
      _.headOption.flatMap { record =>
        record.action match {
          case purge: Action.Purge => Purge(record.offset, purge.origin, purge.version).some
          case _                   => none
        }
      }
    }

    val delete0 = actions.get("D").map { deletes =>
      // take `Delete` action with largest `seqNr`
      val actions = deletes.collect { case ActionRecord(a: Action.Delete, po) => ActionRecord(a, po) } // recover type
      val delete = actions.reduceRight { (a, b) => // CHANGE: use `.reduceRight`
        if (a.action.to.value > b.action.to.value) a
        else {
          // TODO MR here we expect that in `metajournal` we save: highest `seqNr` with first `origin` - is it important?
          //  if not, then alternative code could be simple: `b`
          val origin = a.action.header.origin.orElse(b.action.header.origin)
          b.copy(action = b.action.copy(header = b.action.header.copy(origin = origin)))
        }
      }
      Delete(delete.offset, delete.action.to, delete.action.origin, delete.action.version)
    }

    val appends = actions.get("A").flatMap { appends =>
      // merge all `Append`s
      val deleteTo = delete0.map(_.to.value)
      val actions0 = appends.reverse.collect { // CHANGE: use `.reverse`
        // drop to be deleted `Append`s, except last one - we want to save its expiration in `metajournal`
        case ActionRecord(a: Action.Append, po) if deleteTo.forall(_ <= a.range.to) => ActionRecord(a, po)
      }

      // we can drop first `append`, if `deleteTo` will discard it AND there is at least one more `append`
      val actions = actions0.headOption match {
        case Some(head) if deleteTo.contains(head.action.range.to) && actions0.tail.nonEmpty => actions0.tail
        case _                                                                               => actions0
      }

      NonEmptyList.fromList(actions) match {
        case Some(actions) => Appends(appends.last.offset, actions).some
        case None          => none
      }
    }

    // if `delete` was not last action, adjust `delete`'s batch offset to update `metajournal` correctly
    val delete = appends match {
      case Some(appends) => delete0.map(delete => delete.copy(offset = delete.offset max appends.offset))
      case None          => delete0
    }

    // apply action batches in order: `Purge`, `Append`s and `Delete`
    List(purge, appends, delete).flatten
  }

  final case class Appends(
    offset: Offset,
    records: NonEmptyList[ActionRecord[Action.Append]],
  ) extends Batch_4_1_0_Alternative_with_List_Reverse

  final case class Delete(
    offset: Offset,
    to: DeleteTo,
    origin: Option[Origin],
    version: Option[Version],
  ) extends Batch_4_1_0_Alternative_with_List_Reverse

  final case class Purge(
    offset: Offset,
    origin: Option[Origin],
    version: Option[Version],
  ) extends Batch_4_1_0_Alternative_with_List_Reverse
}
