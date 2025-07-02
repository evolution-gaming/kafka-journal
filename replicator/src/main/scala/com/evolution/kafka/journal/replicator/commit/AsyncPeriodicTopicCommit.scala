package com.evolution.kafka.journal.replicator.commit

import cats.Applicative
import cats.data.{NonEmptyMap, NonEmptySet}
import cats.effect.*
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.evolutiongaming.catshelper.DataHelper.*
import com.evolutiongaming.catshelper.{Log, MonadThrowable}
import com.evolution.kafka.journal.KafkaConsumer
import com.evolution.kafka.journal.replicator.TopicCommit
import com.evolutiongaming.skafka.*
import com.evolutiongaming.skafka.consumer.RebalanceCallback
import com.evolutiongaming.skafka.consumer.RebalanceCallback.syntax.*
import org.apache.kafka.common.errors.RebalanceInProgressException

import scala.concurrent.duration.FiniteDuration

/**
 * @see
 *   [[TopicCommit.asyncPeriodic]]
 */
private[replicator] final class AsyncPeriodicTopicCommit[F[_]: MonadThrowable] private[commit] (
  topic: Topic,
  commitMetadata: Metadata,
  consumer: KafkaConsumer[F, ?, ?],
  stateRef: Ref[F, AsyncPeriodicTopicCommit.State],
  log: Log[F],
) extends TopicCommit[F] {
  override def apply(offsets: NonEmptyMap[Partition, Offset]): F[Unit] = {
    stateRef.update(_.updateMarkedOffsetsForAllAssigned(offsets.toSortedMap))
  }

  override def onPartitionsAssigned(partitions: NonEmptySet[Partition]): RebalanceCallback[F, Unit] = {
    stateRef.update(_.addAssignedPartitions(partitions.toSortedSet)).lift
  }

  override def onPartitionsRevoked(partitions: NonEmptySet[Partition]): RebalanceCallback[F, Unit] = {
    for {
      revokedToCommit <- stateRef.modify { state =>
        val revokedToCommit = state.partitionOffsetsToCommit.view.filterKeys(partitions.contains).toMap
        state.removeAssignedPartitions(partitions.toSortedSet) -> revokedToCommit
      }.lift

      anythingCommitted <-
        commitIfNotEmpty[RebalanceCallback[F, *]](revokedToCommit, commitF = RebalanceCallback.commit)
          .recoverWith {
            case t: Throwable =>
              log.error(
                s"offset commit for revoked partitions on rebalance failed: ${ t.getMessage }",
                t,
              ).lift.as(false)
          }

      _ <-
        if (anythingCommitted)
          log.debug(s"offset commit for revoked partitions on rebalance success: $revokedToCommit").lift
        else RebalanceCallback.empty
    } yield ()
  }

  override def onPartitionsLost(partitions: NonEmptySet[Partition]): RebalanceCallback[F, Unit] = {
    RebalanceCallback.lift(stateRef.update(_.removeAssignedPartitions(partitions.toSortedSet)))
  }

  // opened to the package for tests
  private[commit] def periodicCommitStep(): F[Unit] = {
    for {
      state <- stateRef.get
      offsetsToCommit = state.partitionOffsetsToCommit

      anythingCommitted <- commitIfNotEmpty[F](offsetsToCommit, commitF = consumer.commitLater)
        .recoverWith(matchConsumerError {
          case _: RebalanceInProgressException =>
            log.debug("periodic commit failed due to overlapping with a rebalance, waiting until the next attempt")
              .as(false)
        })

      _ <-
        if (anythingCommitted) {
          stateRef.update(_.updateCommittedOffsetsForAllAssigned(offsetsToCommit)) >>
            log.debug(s"periodic offset commit success: $offsetsToCommit")
        } else Applicative[F].unit
    } yield ()
  }

  private def commitIfNotEmpty[G[_]: MonadThrowable](
    offsets: Map[Partition, Offset],
    commitF: NonEmptyMap[TopicPartition, OffsetAndMetadata] => G[Unit],
  ): G[Boolean] = {
    offsets.toNem.fold(Applicative[G].pure(false)) { offsetsNem =>
      commitF(
        offsetsNem
          .mapBoth {
            case (partition, offset) => TopicPartition(topic, partition) -> OffsetAndMetadata(offset, commitMetadata)
          },
      ).as(true)
    }
  }

  /**
   * `kafka.journal.KafkaConsumer` wraps exceptions in `KafkaConsumerError` - to be on the safe
   * side, this method allows handling Java consumer errors both wrapped and unwrapped.
   */
  private def matchConsumerError[T](pf: PartialFunction[Throwable, T]): PartialFunction[Throwable, T] = {
    pf.orElse {
      case e: com.evolution.kafka.journal.KafkaConsumerError if pf.isDefinedAt(e.cause) =>
        pf.apply(e.cause)
    }
  }
}

private[replicator] object AsyncPeriodicTopicCommit {

  def make[F[_]: Temporal](
    topic: Topic,
    commitMetadata: Metadata,
    commitPeriod: FiniteDuration,
    consumer: KafkaConsumer[F, ?, ?],
    log: Log[F],
  ): Resource[F, TopicCommit[F]] = {
    val mkInstanceF: F[AsyncPeriodicTopicCommit[F]] = for {
      stateRef <- Ref.of[F, State](State())
    } yield new AsyncPeriodicTopicCommit[F](
      topic = topic,
      commitMetadata = commitMetadata,
      consumer = consumer,
      stateRef = stateRef,
      log = log,
    )

    for {
      instance <- mkInstanceF.toResource

      periodicCommitProcessF = (
        Temporal[F].sleep(commitPeriod) >>
          instance
            .periodicCommitStep()
            .recoverWith {
              case t: Throwable =>
                log.error(s"periodic offset commit failed: ${ t.getMessage }", t)
            }
      ).foreverM[Unit]

      _ <- periodicCommitProcessF.background
    } yield instance
  }

  // opened to the package for tests
  private[commit] final case class State(
    assignedPartitions: Map[Partition, PartitionState] = Map.empty,
  ) {
    def addAssignedPartitions(partitions: Iterable[Partition]): State = {
      copy(
        assignedPartitions = partitions.foldLeft(assignedPartitions) { (prevAssigned, partition) =>
          prevAssigned.updatedWith(partition)(prevStateOpt => prevStateOpt.getOrElse(PartitionState()).some)
        },
      )
    }

    def removeAssignedPartitions(partitions: Iterable[Partition]): State = {
      copy(assignedPartitions = assignedPartitions.removedAll(partitions))
    }

    def updateCommittedOffsetsForAllAssigned(offsets: Iterable[(Partition, Offset)]): State = {
      updateForAllAssigned(offsets)(_.updateCommittedIfNeeded(_))
    }

    def updateMarkedOffsetsForAllAssigned(offsets: Iterable[(Partition, Offset)]): State = {
      updateForAllAssigned(offsets)(_.updateMarkedIfNeeded(_))
    }

    def partitionOffsetsToCommit: Map[Partition, Offset] = {
      assignedPartitions
        .view
        .flatMap {
          case (partition, partitionState) => partitionState.needToCommit.map(partition -> _)
        }
        .toMap
    }

    private def updateForAllAssigned(
      offsets: Iterable[(Partition, Offset)],
    )(
      f: (PartitionState, Offset) => PartitionState,
    ): State = {
      copy(
        assignedPartitions = offsets.foldLeft(assignedPartitions) {
          case (prevAssignedPartitions, (partition, offset)) =>
            prevAssignedPartitions.updatedWith(partition)(partitionStateOpt => partitionStateOpt.map(f(_, offset)))
        },
      )
    }
  }

  // opened to the package for tests
  private[commit] final case class PartitionState(
    markedOffsetOpt: Option[Offset] = None,
    committedOffsetOpt: Option[Offset] = None,
  ) {
    def needToCommit: Option[Offset] = {
      markedOffsetOpt.filter { markedOffset =>
        committedOffsetOpt.fold(true)(committedOffset => markedOffset > committedOffset)
      }
    }

    def updateMarkedIfNeeded(newMarkedOffset: Offset): PartitionState = {
      markedOffsetOpt match {
        case Some(markedOffset) if markedOffset >= newMarkedOffset =>
          this
        case _ =>
          copy(markedOffsetOpt = Some(newMarkedOffset))
      }
    }

    def updateCommittedIfNeeded(newCommittedOffset: Offset): PartitionState = {
      committedOffsetOpt match {
        case Some(committedOffset) if committedOffset >= newCommittedOffset =>
          this
        case _ =>
          copy(committedOffsetOpt = Some(newCommittedOffset))
      }
    }
  }

}
