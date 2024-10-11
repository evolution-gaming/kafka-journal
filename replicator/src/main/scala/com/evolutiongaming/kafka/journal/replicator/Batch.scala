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
 */
private[journal] sealed abstract class Batch extends Product {

  def offset: Offset
}

private[journal] object Batch {

  def of(records: NonEmptyList[ActionRecord[Action]]): List[Batch] = {
// TODO MR   val offset = records.last.partitionOffset.offset

    val actions = records
      .foldLeft(List.empty[ActionRecord[Action]]) {
        // drop all `Mark` actions
        case (acc, ActionRecord(_: Action.Mark, _)) => acc

        // drop all actions before `Purge`
        case (_, r @ ActionRecord(_: Action.Purge, _)) => List(r)

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
// TODO MR          case purge: Action.Purge => Purge(offset, purge.origin, purge.version).some
          case _                   => none
        }
      }
    }

    val delete = actions.get("D").map { deletes =>
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
// TODO MR     Delete(offset, delete.action.to, delete.action.origin, delete.action.version)
    }

    val appends = actions.get("A").flatMap { appends =>
      // merge all `Append`s
      val deleteTo = delete.map(_.to.value)
      val actions = appends.collect {
        // drop to be deleted `Append`s
        case ActionRecord(a: Action.Append, po) if deleteTo.forall(_ <= a.range.to) => ActionRecord(a, po)
      }
      NonEmptyList.fromList(actions) match {
        case Some(actions) => Appends(appends.last.offset, actions).some
// TODO MR       case Some(actions) => Appends(offset, actions).some
        case None          => none
      }
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
