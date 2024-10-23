package com.evolutiongaming.kafka.journal.replicator

import cats.data.NonEmptyList
import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.*
import com.evolutiongaming.skafka.Offset

/**
 * Copy of `Batch` with changes:
 *  - change batching algorithm so it is easier to comprehend
 *  - records are aggregated within `Vector` to make append faster (line: 31)
 */
private[journal] sealed abstract class Batch_4_1_0_Alternative_with_groupping extends Product {

  def offset: Offset
}

private[journal] object Batch_4_1_0_Alternative_with_groupping {

  def of(records: NonEmptyList[ActionRecord[Action]]): List[Batch_4_1_0_Alternative_with_groupping] = {

    val actions = records.groupMapReduceWith {
      _.action match {
        case _: Action.Mark   => "M"
        case _: Action.Purge  => "P"
        case _: Action.Delete => "D"
        case _: Action.Append => "A"
      }
    } {
      case ActionRecord(_: Action.Mark, _)         => none[Batch_4_1_0_Alternative_with_groupping]
      case ActionRecord(purge: Action.Purge, po)   => Purge(po.offset, purge.origin, purge.version).some
      case ActionRecord(delete: Action.Delete, po) => Deletes(po.offset, NonEmptyList.of(ActionRecord(delete, po))).some
      case ActionRecord(append: Action.Append, po) => Appends(po.offset, NonEmptyList.of(ActionRecord(append, po))).some
    } { (acc, record) =>
      (acc, record) match {
        case (Some(acc: Purge), Some(record: Purge)) =>
          if (acc.offset < record.offset) record.some
          else acc.some
        case (Some(acc: Deletes), Some(record: Deletes)) =>
          record.copy(records = acc.records ::: record.records).some
        case (Some(acc: Appends), Some(record: Appends)) =>
          record.copy(records = acc.records ::: record.records).some
        case (_, _) =>
          none // cannot happen
      }
    }

    val purge = actions.get("P").flatten

    val delete0 = actions
      .get("D")
      .flatten
      .collect {
        case (deletes: Deletes) =>
          deletes
            .records
            .filter { record => purge.forall(_.offset < record.offset) }
            .reduceLeftOption[ActionRecord[Action.Delete]] {
              case (acc, delete) =>
                if (acc.action.to.value > delete.action.to.value) acc
                else {
                  // TODO MR here we expect that in `metajournal` we save: highest `seqNr` with first `origin` - is it important?
                  //  if not, then alternative code could be simple: `record.some`
                  val origin  = acc.action.origin.orElse(delete.action.origin)
                  val version = acc.action.version.orElse(delete.action.version)
                  delete.copy(action = delete.action.copy(header = delete.action.header.copy(origin = origin, version = version)))
                }
            }
            .map { delete =>
              Delete(delete.offset, delete.action.to, delete.action.origin, delete.action.version)
            }
      }
      .flatten

    val appends = actions
      .get("A")
      .flatten
      .collect {
        case (appends: Appends) =>
          val actions0 = appends
            .records
            .filter { record => purge.forall(_.offset < record.offset) && delete0.forall(_.to.value <= record.action.range.to) }

          val actions = (actions0.headOption, delete0) match {
            case (Some(head), Some(Delete(_, to, _, _))) if to.value == head.action.range.to && actions0.tail.nonEmpty =>
              actions0.tail
            case _ =>
              actions0
          }

          NonEmptyList.fromList(actions) match {
            case Some(actions) => Appends(actions0.last.offset, actions).some
            case None          => none
          }
      }
      .flatten

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
  ) extends Batch_4_1_0_Alternative_with_groupping

  final case class Deletes(
    offset: Offset,
    records: NonEmptyList[ActionRecord[Action.Delete]],
  ) extends Batch_4_1_0_Alternative_with_groupping

  final case class Delete(
    offset: Offset,
    to: DeleteTo,
    origin: Option[Origin],
    version: Option[Version],
  ) extends Batch_4_1_0_Alternative_with_groupping

  final case class Purge(
    offset: Offset,
    origin: Option[Origin],
    version: Option[Version],
  ) extends Batch_4_1_0_Alternative_with_groupping
}
