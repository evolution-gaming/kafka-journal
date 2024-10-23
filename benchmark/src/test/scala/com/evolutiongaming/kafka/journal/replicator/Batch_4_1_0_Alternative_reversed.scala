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
      // TODO MR discard next 2 lines - match on explosion of types
      case (state, record) =>
        (state, record.action) match {
          case (state, _) if state.purge.nonEmpty =>
            state

          case (state, purge: Action.Purge) =>
            state.copy(
              purge = Purge(record.offset, purge.origin, purge.version).some,
            )

          case (state, delete: Action.Delete) =>
            state.delete match {
              case Some(younger) =>
                // take `origin` and `version` from "older" entity, if it has them
                val origin  = delete.origin.orElse(younger.origin)
                val version = delete.version.orElse(younger.version)
                if (younger.to < delete.to) {
                  state.copy(
                    delete = Delete(record.offset, delete.to, origin, version).some,
                  )
                } else {
                  state.copy(
                    delete = Delete(younger.offset, younger.to, origin, version).some,
                  )
                }
              case None =>
                state.copy(
                  delete = Delete(record.offset, delete.to, delete.origin, delete.version).some,
                )
            }

          case (state, append: Action.Append) if state.delete.forall(_.to.value <= append.range.to) =>
            state.appends match {
              case Some(appends) =>
                Appends(record.offset, NonEmptyList.of(ActionRecord(append, record.partitionOffset))).some
                state.copy(
                  appends = appends
                    .copy(
                      records = ActionRecord(append, record.partitionOffset) :: appends.records,
                    )
                    .some,
                )
              case None =>
                state.copy(
                  appends = Appends(record.offset, NonEmptyList.of(ActionRecord(append, record.partitionOffset))).some,
                )
            }

          case (state, _) => // ignore actions `Mark` and `Append`, if it would get deleted
            state
        }
    }

    state.batches
  }

  private case class State(
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
