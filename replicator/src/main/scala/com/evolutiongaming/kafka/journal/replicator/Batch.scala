package com.evolutiongaming.kafka.journal.replicator

import cats.data.NonEmptyList
import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.*
import com.evolutiongaming.skafka.Offset

/**
 * Receives list of records from [[ReplicateRecords]], groups and optimizes similar or sequential actions, like:
 *  - two or more `append` or `delete` actions are merged into one
 *  - optimizes list of records to minimize load on Cassandra, like:
 *    - aggregate effective actions by processing them from last record down to first
 *    - ignore/drop all actions, which are before last `Purge`
 *    - ignore all `Mark` actions
 *    - if `append`(s) are followed by `delete` all `append`(s), except last, are dropped
 *    - at the end, apply all aggregated batches following order: `purge`, `append`s, `delete`
 */
private[journal] sealed abstract class Batch extends Product {

  def offset: Offset
}

private[journal] object Batch {

  def of(records: NonEmptyList[ActionRecord[Action]]): List[Batch] = {

    val state = records.reverse.foldLeft(State()) {
      // ignore all actions before `Purge`
      case (state, _) if state.purge.nonEmpty =>
        state

      case (state, ActionRecord(purge: Action.Purge, po)) =>
        state.copy(
          purge = Purge(po.offset, purge.origin, purge.version).some,
        )

      case (state, ActionRecord(delete: Action.Delete, po)) =>
        val delete_ = state.delete match {
          case Some(younger) =>
            // take `origin` and `version` from "older" entity, if it has them
            val origin  = delete.origin.orElse(younger.origin)
            val version = delete.version.orElse(younger.version)
            // make `Delete` action with largest `seqNr` and largest `offset`
            if (younger.to < delete.to) Delete(po.offset, delete.to, origin, version)
            else younger.copy(origin = origin, version = version)

          case None =>
            Delete(po.offset, delete.to, delete.origin, delete.version)
        }
        state.copy(
          delete = delete_.some,
        )

      case (state, ActionRecord(append: Action.Append, po)) if state.delete.forall(_.to.value <= append.range.to) =>
        val appends = state.appends match {
          case Some(appends) =>
            appends.copy(records = ActionRecord(append, po) :: appends.records)
          case None =>
            Appends(po.offset, NonEmptyList.of(ActionRecord(append, po)))
        }
        state.copy(
          appends = appends.some,
        )

      case (state, _) =>
        // ignore `Action.Append`, if it would get deleted and ignore `Action.Mark`
        state
    }

    state.batches
  }

  private final case class State(
    purge: Option[Purge]     = None,
    appends: Option[Appends] = None,
    delete: Option[Delete]   = None,
  ) {

    def batches: List[Batch] = {
      // we can drop first `append`, if `deleteTo` will discard it AND there is at least one more `append`
      // we have to preserve one `append` to store latest `seqNr` and populate `expireAfter`
      val appends = {
        this.appends.flatMap { appends =>
          val deleteTo = this.delete.map(_.to.value)
          val records  = appends.records
          val actions =
            if (deleteTo.contains(records.head.action.range.to)) NonEmptyList.fromList(records.tail).getOrElse(records)
            else records
          appends.copy(records = actions).some
        }
      }

      // if `delete` was not last action, adjust `delete`'s batch offset to update `metajournal` correctly
      val delete = appends match {
        case Some(appends) => this.delete.map(delete => delete.copy(offset = delete.offset max appends.offset))
        case None          => this.delete
      }

      // apply action batches in order: `Purge`, `Append`s and `Delete`
      List(purge, appends, delete).flatten
    }
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
    origin: Option[Origin], // used only for logging
    version: Option[Version], // used only for logging
  ) extends Batch
}
