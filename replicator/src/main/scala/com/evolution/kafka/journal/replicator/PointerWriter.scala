package com.evolution.kafka.journal.replicator

import cats.data.{NonEmptyMap as Nem, NonEmptySet as Nes}
import cats.effect.*
import cats.effect.implicits.*
import cats.effect.std.Semaphore
import cats.syntax.all.*
import com.evolution.kafka.journal.eventual.ReplicatedPartitionJournal
import com.evolution.kafka.journal.util.SkafkaHelper.*
import com.evolutiongaming.catshelper.DataHelper.*
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.retry.Sleep
import com.evolutiongaming.skafka.{Offset, Partition, Topic, TopicPartition}

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

/**
 * Writes partition pointers (`pointer2`) to the eventual store and feeds the Kafka commit from
 * acknowledged pointer writes only.
 *
 * At the end of a round, the replicated offsets are only [[mark]]ed. Depending on the
 * implementation, the pointer is then written within the same call ([[PointerWriter.Of.sync]]) or
 * later by a background fiber ([[PointerWriter.Of.periodic]]). The latter lets the next poll start
 * without waiting for Cassandra.
 *
 * Guarantees, see `docs/throughput/invariants.md`:
 *   - I3: only offsets of finished rounds are marked, so the pointer never passes unfinished work.
 *     Pointer writes are serialised, so the pointer never moves backwards.
 *   - I4: an offset is handed to [[TopicCommit]] only once a pointer at or above it is acknowledged
 *     by Cassandra, or was already stored there.
 *   - I5: a failed background write fails the next [[mark]], which tears down the consumer.
 *   - I6: [[revoke]] writes the pending pointers of the revoked partitions before it returns.
 *
 * [[ReplicatedOffsetNotifier]] is called after a pointer write is acknowledged.
 */
private[journal] trait PointerWriter[F[_]] {

  /**
   * Registers a partition with the pointer that is currently stored for it. May be called again for
   * a known partition, e.g. after its partition flow expired from the cache; the known state is
   * then kept.
   *
   * @param offsets
   *   access to the stored pointer of the partition
   * @param stored
   *   pointer read from `offsets`
   * @return
   *   offset up to which (inclusive) all records of the partition are replicated, including marked
   *   offsets not written yet. Records at or below it can be skipped.
   */
  def assign(
    partition: Partition,
    offsets: ReplicatedPartitionJournal.Offsets[F],
    stored: Option[Offset],
  ): F[Option[Offset]]

  /**
   * Marks offsets as replicated: all records of a partition up to and including the offset are
   * fully applied. Offsets of unknown partitions are ignored.
   *
   * Fails if a background pointer write has failed.
   */
  def mark(offsets: Nem[Partition, Offset], timestamp: Instant): F[Unit]

  /**
   * Writes the pending pointers of the partitions, hands them to the commit and forgets the
   * partitions. Waits for a pointer write that is in flight.
   */
  def revoke(partitions: Nes[Partition]): F[Unit]

  /**
   * Forgets the partitions without writing their pointers.
   */
  def lose(partitions: Nes[Partition]): F[Unit]
}

private[journal] object PointerWriter {

  private[journal] trait Of[F[_]] {

    def createWriter(
      topic: Topic,
      commit: TopicCommit[F],
      notifier: ReplicatedOffsetNotifier[F],
    ): Resource[F, PointerWriter[F]]
  }

  private[journal] object Of {

    /**
     * Writes pointers within [[PointerWriter.mark]], i.e., on the critical path of a round.
     */
    def sync[F[_]: Concurrent]: Of[F] = new SyncOf[F]

    /**
     * Writes pointers in a background fiber: sleeps for `period`, writes all pending pointers in
     * parallel, repeats. Updates marked while a write is in flight are coalesced into the next one.
     */
    def periodic[F[_]: Concurrent: Sleep](period: FiniteDuration, log: Log[F]): Of[F] =
      new PeriodicOf[F](period, log)
  }

  private final class SyncOf[F[_]: Concurrent] extends Of[F] {

    def createWriter(
      topic: Topic,
      commit: TopicCommit[F],
      notifier: ReplicatedOffsetNotifier[F],
    ): Resource[F, PointerWriter[F]] = {
      Main.make(topic, commit, notifier, writeOnMark = true).toResource
    }
  }

  private final class PeriodicOf[F[_]: Concurrent: Sleep](period: FiniteDuration, log: Log[F]) extends Of[F] {

    def createWriter(
      topic: Topic,
      commit: TopicCommit[F],
      notifier: ReplicatedOffsetNotifier[F],
    ): Resource[F, PointerWriter[F]] = {
      for {
        writer <- Main.make(topic, commit, notifier, writeOnMark = false).toResource
        _ <- writer.writePeriodically(period, log).background
      } yield writer
    }
  }

  // opened to the package for tests
  private[replicator] final class Main[F[_]: Concurrent](
    topic: Topic,
    commit: TopicCommit[F],
    notifier: ReplicatedOffsetNotifier[F],
    stateRef: Ref[F, Map[Partition, PartitionState[F]]],
    lock: Semaphore[F],
    failureRef: Ref[F, Option[Throwable]],
    writeOnMark: Boolean,
  ) extends PointerWriter[F] {

    def assign(
      partition: Partition,
      offsets: ReplicatedPartitionJournal.Offsets[F],
      stored: Option[Offset],
    ): F[Option[Offset]] = {
      stateRef.modify { state =>
        val partitionState = state
          .get(partition)
          .fold(PartitionState(offsets = offsets, written = stored)) { _.copy(offsets = offsets).onWritten(stored) }
        (state.updated(partition, partitionState), partitionState.replicated)
      }
    }

    def mark(offsets: Nem[Partition, Offset], timestamp: Instant): F[Unit] = {
      for {
        failure <- failureRef.get
        _ <- failure.traverse_ { _.raiseError[F, Unit] }
        _ <- stateRef.update { state =>
          offsets.toSortedMap.foldLeft(state) {
            case (state, (partition, offset)) =>
              state.updatedWith(partition) { _.map { _.onMarked(offset, timestamp) } }
          }
        }
        _ <- if (writeOnMark) step else ().pure[F]
      } yield ()
    }

    def revoke(partitions: Nes[Partition]): F[Unit] = {
      lock.permit.surround {
        flush(partitions.some) >> stateRef.update { _.removedAll(partitions.toSortedSet) }
      }
    }

    def lose(partitions: Nes[Partition]): F[Unit] = {
      stateRef.update { _.removedAll(partitions.toSortedSet) }
    }

    // opened to the package for tests
    private[replicator] def step: F[Unit] = lock.permit.surround { flush(none) }

    def writePeriodically(
      period: FiniteDuration,
      log: Log[F],
    )(implicit
      sleep: Sleep[F],
    ): F[Unit] = {
      (sleep.sleep(period) >> step)
        .foreverM[Unit]
        .handleErrorWith { error =>
          log.error(s"pointer write failed, replication will be restarted: $error", error) >>
            failureRef.set(error.some)
        }
    }

    /**
     * Must be called holding `lock`, so that writes of one partition never overlap.
     */
    private def flush(partitions: Option[Nes[Partition]]): F[Unit] = {

      def selected(state: Map[Partition, PartitionState[F]]): Map[Partition, PartitionState[F]] = {
        partitions.fold(state) { partitions =>
          state.filter { case (partition, _) => partitions.contains(partition) }
        }
      }

      for {
        state <- stateRef.get
        writes = selected(state).toList.flatMap {
          case (partition, partitionState) =>
            partitionState.pendingWrite.map { marked => (partition, partitionState, marked) }
        }
        _ <- writes.parTraverse_ { case (partition, partitionState, marked) =>
          write(partition, partitionState, marked)
        }
        toCommit <- stateRef.modify { state =>
          val toCommit = selected(state).toList.flatMap {
            case (partition, partitionState) => partitionState.pendingCommit.map { offset => (partition, offset) }
          }
          val state1 = toCommit.foldLeft(state) {
            case (state, (partition, offset)) =>
              state.updatedWith(partition) { _.map { _.onCommitted(offset) } }
          }
          (state1, toCommit)
        }
        toCommit <- toCommit.traverse { case (partition, offset) => offset.inc[F].tupleLeft(partition) }
        _ <- toCommit.toMap.toNem.traverse_ { offsets => commit(offsets) }
      } yield ()
    }

    private def write(partition: Partition, partitionState: PartitionState[F], marked: Marked): F[Unit] = {
      val offsets = partitionState.offsets
      for {
        _ <- partitionState
          .written
          .fold { offsets.create(marked.offset, marked.timestamp) } { _ =>
            offsets.update(marked.offset, marked.timestamp)
          }
        _ <- stateRef.update { _.updatedWith(partition) { _.map { _.onWritten(marked.offset.some) } } }
        _ <- notifier.onReplicatedOffset(TopicPartition(topic, partition), marked.offset)
      } yield ()
    }
  }

  private[replicator] object Main {

    def make[F[_]: Concurrent](
      topic: Topic,
      commit: TopicCommit[F],
      notifier: ReplicatedOffsetNotifier[F],
      writeOnMark: Boolean,
    ): F[Main[F]] = {
      for {
        stateRef <- Ref.of[F, Map[Partition, PartitionState[F]]](Map.empty)
        lock <- Semaphore[F](1)
        failureRef <- Ref.of[F, Option[Throwable]](none)
      } yield new Main[F](
        topic = topic,
        commit = commit,
        notifier = notifier,
        stateRef = stateRef,
        lock = lock,
        failureRef = failureRef,
        writeOnMark = writeOnMark,
      )
    }
  }

  private[replicator] final case class Marked(offset: Offset, timestamp: Instant)

  /**
   * @param written
   *   last pointer stored in Cassandra: read at assignment or acknowledged after a write
   * @param marked
   *   last offset of a finished round
   * @param committed
   *   last marked offset handed to the commit
   */
  private[replicator] final case class PartitionState[F[_]](
    offsets: ReplicatedPartitionJournal.Offsets[F],
    written: Option[Offset],
    marked: Option[Marked] = none,
    committed: Option[Offset] = none,
  ) {

    def replicated: Option[Offset] = (written.toList ++ marked.map { _.offset }).maxOption

    def pendingWrite: Option[Marked] = marked.filter { marked => written.forall { _ < marked.offset } }

    def pendingCommit: Option[Offset] = {
      marked.map { _.offset }.filter { offset =>
        written.exists { _ >= offset } && committed.forall { _ < offset }
      }
    }

    def onMarked(offset: Offset, timestamp: Instant): PartitionState[F] = {
      if (marked.exists { _.offset >= offset }) this else copy(marked = Marked(offset, timestamp).some)
    }

    def onWritten(offset: Option[Offset]): PartitionState[F] = {
      offset.fold(this) { offset =>
        if (written.exists { _ >= offset }) this else copy(written = offset.some)
      }
    }

    def onCommitted(offset: Offset): PartitionState[F] = {
      if (committed.exists { _ >= offset }) this else copy(committed = offset.some)
    }
  }
}
