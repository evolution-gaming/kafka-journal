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
private[journal] sealed abstract class Batch_4_1_0_Alternative_reversed extends Product {

  def offset: Offset
}

private[journal] object Batch_4_1_0_Alternative_reversed {

  def of(records: NonEmptyList[ActionRecord[Action]]): List[Batch_4_1_0_Alternative_reversed] = {

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

    def batches: List[Batch_4_1_0_Alternative_reversed] = {
      // we can drop first `append`, if `deleteTo` will discard it AND there is at least one more `append`
      val appends = {
        this.appends match {
          case Some(appends) =>
            val deleteTo = this.delete.map(_.to.value)
            val records  = appends.records
            val actions =
              if (deleteTo.contains(records.head.action.range.to) && records.tail.nonEmpty) appends.records.tail
              else appends.records.toList
            NonEmptyList.fromList(actions) match {
              case Some(actions) => appends.copy(records = actions).some
              case None          => appends.some // cannot happen
            }

          case None =>
            None
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
  ) extends Batch_4_1_0_Alternative_reversed

  final case class Delete(
    offset: Offset,
    to: DeleteTo,
    origin: Option[Origin],
    version: Option[Version],
  ) extends Batch_4_1_0_Alternative_reversed

  final case class Purge(
    offset: Offset,
    origin: Option[Origin],
    version: Option[Version],
  ) extends Batch_4_1_0_Alternative_reversed
}
