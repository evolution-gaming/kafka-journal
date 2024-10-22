package com.evolutiongaming.kafka.journal.replicator

import cats.data.NonEmptyList
import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.*
import com.evolutiongaming.skafka.Offset

/**
 * Receives list of records from [[ReplicateRecords]], groups and optimizes similar or sequential actions, like:
 *  - two or more `append` or `delete` actions are merged into one
 *  - optimizes list of records to minimize load on Cassandra, like:
 *    - if `append`(s) are followed by `delete` all `append`(s), except last, are dropped
 *    - if `append`(s) are followed by `prune`, then all `append`(s) are dropped
 *    - `mark` actions are ignored
 *    - at the end, apply in following order: `purge`, `append`s, `delete`
 */
private[journal] sealed abstract class Batch extends Product {

  def offset: Offset
}

private[journal] object Batch {

  def of(records: NonEmptyList[ActionRecord[Action]]): List[Batch] = {

    val actions = records
      .foldLeft(Vector.empty[ActionRecord[Action]]) {
        // drop all `Mark` actions
        case (acc, ActionRecord(_: Action.Mark, _)) => acc

        // drop all actions before `Purge`
        case (_, r @ ActionRecord(_: Action.Purge, _)) => Vector(r)

        // collect `Append` and `Delete` actions
        case (acc, record) => acc :+ record
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
      val delete = actions.reduceLeft { (a, b) =>
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
      val actions0 = appends.collect {
        // drop to be deleted `Append`s, except last one - we want to save its expiration in `metajournal`
        case ActionRecord(a: Action.Append, po) if deleteTo.forall(_ <= a.range.to) => ActionRecord(a, po)
      }

      // we can drop first `append`, if `deleteTo` will discard it AND there is at least one more `append`
      val actions = actions0.headOption match {
        case Some(head) if deleteTo.contains(head.action.range.to) && actions0.tail.nonEmpty => actions0.tail
        case _                                                                               => actions0
      }

      NonEmptyList.fromList(actions.toList) match {
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
  ) extends Batch

  final case class Delete(
    offset: Offset,
    to: DeleteTo,
    origin: Option[Origin],
    version: Option[Version],
  ) extends Batch

  final case class Purge(
    offset: Offset,
    origin: Option[Origin],
    version: Option[Version],
  ) extends Batch
}
