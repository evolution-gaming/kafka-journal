package com.evolutiongaming.kafka.journal.replicator

import cats.data.NonEmptyList
import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.*
import com.evolutiongaming.skafka.Offset

/**
 * Receives list of records from [[ReplicateRecords]], groups and optimizes similar or sequential actions, like:
 *  - two or more `append` or `delete` actions are merged into one
 *  - optimizes list of records to minimize load on Cassandra, like:
 *    - aggregate effective actions by processing them from the youngest record down to oldest
 *    - ignore/drop all actions, which are before last `Purge`
 *    - ignore all `Mark` actions
 *    - if `append`(s) are followed by `delete` all `append`(s), except last, are dropped
 *    - at the end, apply all aggregated batches following order: `purge`, `append`s, `delete`
 *  Our goal is to minimize load on Cassandra and t achieve it we want to execute no more than 3 operations.
 *  The order: `purge`, `appends` and `delete` provides the most compact structure:
 *   - in case of `purge` we have to execute it first - as following operations have to start a new journal
 *   - we cannot `delete` future `append`s, thus `delete` must be after `append`
 *
 * Assumptions:
 *  - client doesn't abuse `Delete(MAX)` or `Delete(SeqNr + X)` which gets clamped down to `SeqNr` in
 *    [[com.evolutiongaming.kafka.journal.eventual.cassandra.ReplicatedCassandra]]
 *  - `Delete` action before `Append` can be moved after `Append`
 *  - client issues `Delete` and following `Append` actions with same `origin` and `version` fields - important only
 *    when starting a new journal (or right after `Purge`) - it has no technical effect
 */
private[journal] sealed abstract class Batch extends Product {

  def offset: Offset
}

private[journal] object Batch {

  def of(records: NonEmptyList[ActionRecord[Action]]): List[Batch] = {
    records
      .reverse
      .foldLeft { State.empty } { State.fold }
      .batches
  }

  private case class State(batches: List[Batch]) {

    def next: Option[Batch] = batches.headOption

    def delete: Option[Delete] = batches.collectFirst { case d: Delete => d }

    def prepend(batch: Batch): State = new State(batch :: batches)

    def replace(batch: Batch): State = new State(batch :: batches.tail)

  }

  private object State {

    val empty: State = State(List.empty)

    def fold(state: State, event: ActionRecord[Action]): State = event match {

      case ActionRecord(_: Action.Mark, _) => state

      case ActionRecord(purge: Action.Purge, partitionOffset: PartitionOffset) =>
        def purgeBatch = Purge(partitionOffset.offset, purge.origin, purge.version)

        state.next match {
          case Some(_: Purge) => state
          case Some(_)        => state.prepend(purgeBatch)
          case None           => state.prepend(purgeBatch)
        }

      case ActionRecord(delete: Action.Delete, partitionOffset: PartitionOffset) =>
        def deleteBatch(delete: Action.Delete) =
          Delete(partitionOffset.offset, delete.to, delete.origin, delete.version)

        state.next match {

          case Some(_: Purge) => state
          case None           => state.prepend(deleteBatch(delete))

          case Some(_: Appends) =>
            // if `delete` included in `state.delete` then ignore it
            val delete1 = state.delete match {
              case None          => delete.some
              case Some(delete1) => if (delete1.to.value < delete.to.value) delete.some else None
            }
            delete1 match {
              case Some(delete) => state.prepend(deleteBatch(delete))
              case None         => state
            }

          case Some(next: Delete) =>
            if (delete.header.to.value < next.to.value) state
            // if `delete` includes `next` then replace `next` with `delete`
            else state.replace(deleteBatch(delete))
        }

      case ActionRecord(append: Action.Append, partitionOffset: PartitionOffset) =>
        state.next match {

          case Some(_: Purge) => state

          case Some(next: Appends) =>
            val append1 =
              // if `append` deleted by `state.delete` then ignore it
              state.delete match {
                case Some(delete) => if (delete.to.value < append.range.to) append.some else None
                case None         => append.some
              }

            append1 match {
              case Some(append) =>
                val record  = ActionRecord(append, partitionOffset)
                val appends = Appends(next.offset, record :: next.records)
                // replace head (aka [state.next]) with new Appends, i.e. merge `append` with `next`
                state.replace(appends)

              case None => state
            }

          case Some(next: Delete) =>
            val record  = ActionRecord(append, partitionOffset)
            val appends = Appends(partitionOffset.offset, NonEmptyList.one(record))
            state.prepend(appends)

          case None =>
            val record  = ActionRecord(append, partitionOffset)
            val appends = Appends(partitionOffset.offset, NonEmptyList.one(record))
            state.prepend(appends)
        }
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
