package com.evolutiongaming.kafka.journal.replicator

import cats.data.NonEmptyList
import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.*
import com.evolutiongaming.skafka.Offset

/**
 * Copy of `Batch` from 4.1.2:
 *  - change batching algorithm based on some assumptions
 */
private[journal] sealed abstract class Batch_4_1_2 extends Product {

  def offset: Offset
}

private[journal] object Batch_4_1_2 {

  def of(records: NonEmptyList[ActionRecord[Action]]): List[Batch_4_1_2] = {
    State(records).batches
  }

  /** Builds minimal set of actions, which will execute fewer calls to Cassandra while producing the same result */
  private object State {
    def apply(records: NonEmptyList[ActionRecord[Action]]): State = {
      records.reverse.foldLeft(State()) { _.handle(_) }
    }
  }

  private final case class State(
                                  private val purge: Option[Purge]     = None,
                                  private val appends: Option[Appends] = None,
                                  private val delete: Option[Delete]   = None,
                                ) {
    // Expects records to be provided in revered order, e.g. youngest first
    private def handle: ActionRecord[Action] => State = {
      case _ if this.purge.nonEmpty => // ignore all actions before `Purge`
        this

      case ActionRecord(_: Action.Mark, _) =>
        this

      case ActionRecord(purge: Action.Purge, partitionOffset: PartitionOffset) =>
        handlePurge(purge, partitionOffset)

      case ActionRecord(delete: Action.Delete, partitionOffset: PartitionOffset) =>
        handleDelete(delete, partitionOffset)

      case ActionRecord(append: Action.Append, partitionOffset: PartitionOffset) =>
        handleAppend(append, partitionOffset)
    }

    private def handlePurge(purge: Action.Purge, partitionOffset: PartitionOffset): State = {
      this.copy(
        purge = Purge(partitionOffset.offset, purge.origin, purge.version).some,
      )
    }

    private def handleDelete(delete: Action.Delete, partitionOffset: PartitionOffset): State = {
      val delete_ = this.delete match {
        case Some(younger) =>
          // take `origin` and `version` from "older" entity, if it has them
          val origin  = delete.origin.orElse(younger.origin)
          val version = delete.version.orElse(younger.version)
          // make `Delete` action with largest `seqNr` and largest `offset`
          if (younger.to < delete.to) Delete(partitionOffset.offset, delete.to, origin, version)
          else younger.copy(origin = origin, version = version)

        case None =>
          Delete(partitionOffset.offset, delete.to, delete.origin, delete.version)
      }
      this.copy(
        delete = delete_.some,
      )
    }

    private def handleAppend(append: Action.Append, partitionOffset: PartitionOffset): State = {
      // ignore `Action.Append`, if it would get deleted
      if (this.delete.forall(_.to.value <= append.range.to)) {
        val appends = this.appends match {
          case Some(appends) =>
            appends.copy(records = ActionRecord(append, partitionOffset) :: appends.records)
          case None =>
            Appends(partitionOffset.offset, NonEmptyList.of(ActionRecord(append, partitionOffset)))
        }
        this.copy(
          appends = appends.some,
        )
      } else {
        this
      }
    }

    def batches: List[Batch_4_1_2] = {
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
  ) extends Batch_4_1_2

  final case class Delete(
    offset: Offset,
    to: DeleteTo,
    origin: Option[Origin],
    version: Option[Version],
  ) extends Batch_4_1_2

  final case class Purge(
    offset: Offset,
    origin: Option[Origin],
    version: Option[Version],
  ) extends Batch_4_1_2
}
