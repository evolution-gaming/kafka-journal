package com.evolutiongaming.kafka.journal.replicator

import cats.data.NonEmptyList
import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.*
import com.evolutiongaming.skafka.Offset

/**
 * Receives list of records from [[ReplicateRecords]], groups and optimizes similar or sequential actions, like:
 *  - two or more `append` or `delete` actions are merged into one
 *  - optimizes list of records to minimize load on Cassandra, like:
 *    - aggregate effective actions by processing them from the _youngest_ record down to oldest
 *    - ignore/drop all actions, which are before last `Purge`
 *    - ignore all `Mark` actions
 *    - if `append`(s) are followed by `delete` all `append`(s), except last, are dropped
 *  Our goal is to minimize load on Cassandra while preserving original offset ordering.
 */
private[journal] sealed abstract class Batch extends Product {

  def offset: Offset
}

private[journal] object Batch {

  def of(records: NonEmptyList[ActionRecord[Action]]): List[Batch] = {
    // reverse list of records to process them from the youngest record down to oldest
    records
      .reverse
      .foldLeft { State.empty } { State.fold }
      .batches
  }

  /**
    * Internal state used to collapse actions (aka Kafka records) into batches.
    * Each batch represents an action to be applied to Cassandra: `appends`, `delete` or `purge`.
    *
    * @param batches list of batches, where head is the _oldest_ batch
    */
  private case class State(batches: List[Batch]) {

    /**
      * Oldest batch from the state.
      * 
      * Actions are processed from the _youngest_ record down to oldest, so `state.next` on each step represents batch that follows the current action:
      * {{{
      * // a<x> - action #x
      * // b<x> - batch #x
      * [a1,a2,a3,a4,a5,a6,a7] // actions
      * [b6,b7] // state.batches after processing actions a7 & a6
      * state.next == b6 // while processing a5
      * }}}  
      */
    def next: Option[Batch] = batches.headOption

    /**
      * Find _oldest_ delete batch in the state.
      */
    def delete: Option[Delete] = batches.collectFirst { case d: Delete => d }

    /**
      * Add new batch to the state as the _oldest_ one.
      */
    def prepend(batch: Batch): State = new State(batch :: batches)

    /**
      * Replace _oldest_ batch in the state with new one.
      */
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

          // Action is `delete` and next batch is `appends`.
          // Can be that another `delete`, that also deletes same (or more) as incoming `delete` action,
          // present in state's batches - then ignore incoming `delete`.
          case Some(_: Appends) =>
            // If `delete` included in `state.delete` then ignore it
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
            // If `delete` includes `next` then replace `next` with `delete`
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
