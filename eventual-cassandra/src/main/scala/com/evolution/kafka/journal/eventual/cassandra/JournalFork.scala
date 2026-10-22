package com.evolution.kafka.journal.eventual.cassandra

import cats.syntax.all.*
import com.evolution.kafka.journal.{EventRecord, Key, Origin, PartitionOffset, SeqNr}

/**
 * A possible journal fork: an event with a repeated or out-of-order `seqNr`, compared to the
 * journal head and to the earlier events of the same batch.
 *
 * For example:
 *   1. writer A appends events up to `seqNr` 5
 *   2. writer B, a second instance of the entity on another node, recovers up to `seqNr` 5
 *   3. A, not knowing about B, appends `seqNr` 6
 *   4. B appends a different event with `seqNr` 6
 *
 * Both events are stored, as `(seq_nr, timestamp)` is the clustering key of the `journal` table,
 * and the next recovery of the entity may fail with `Data integrity violated: seqNr ... duplicated
 * in multiple records`, see [[com.evolution.kafka.journal.eventual.cassandra.EventualCassandra]].
 *
 * @param detectedAtOffset
 *   where the event with a repeated or out-of-order `seqNr` is in Kafka. One Kafka record often
 *   holds several events, so several forks may have the same offset.
 * @param conflictsWithOffset
 *   where the record the event is compared with is in Kafka: the journal head (the offset of its
 *   last append or delete), or, if there is one, the last event before it in the same batch which
 *   was not a fork
 * @param origin
 *   the origin of the event at `detectedAtOffset`
 */
private[journal] final case class JournalFork(
  key: Key,
  detectedAtOffset: PartitionOffset,
  detectedAtSeqNr: SeqNr,
  conflictsWithOffset: PartitionOffset,
  conflictsWithSeqNr: SeqNr,
  origin: Option[Origin],
) {

  /**
   *   - `true` if the event repeats the `seqNr` of the record it is compared with
   *   - `false` if its `seqNr` is lower: it may still be a duplicate, but it may also be a valid
   *     event written out of order, e.g. by parallel appends to the same journal
   */
  def isDuplicate: Boolean = detectedAtSeqNr == conflictsWithSeqNr

  def show: String = {
    val originStr = origin.foldMap { origin => s", origin: $origin" }
    s"key: $key, detectedAtOffset: $detectedAtOffset, detectedAtSeqNr: $detectedAtSeqNr, " +
      s"conflictsWithOffset: $conflictsWithOffset, conflictsWithSeqNr: $conflictsWithSeqNr$originStr"
  }
}

private[journal] object JournalFork {

  /**
   * Finds the forks among `events`, in event order.
   *
   * Events at or below the offset of `journalHead` are skipped: they were replicated already, and
   * delivering them again is not a fork.
   *
   * @param events
   *   the events of consecutive appends of one journal from one Kafka `poll`, in offset order. They
   *   may come from several Kafka records written by different writers, so both events of a fork
   *   can be among them.
   */
  def fromEvents[A](
    key: Key,
    journalHead: Option[JournalHead],
    events: List[EventRecord[A]],
  ): List[JournalFork] = {

    val newEvents = journalHead.fold(events) { journalHead =>
      events.filter { event => event.partitionOffset.offset > journalHead.partitionOffset.offset }
    }

    val state0 = journalHead.map { journalHead => (journalHead.partitionOffset, journalHead.seqNr) }
    val forks0 = List.empty[JournalFork]

    val (_, forks) = newEvents.foldLeft((state0, forks0)) {
      case ((state, forks), event) =>
        state match {
          case Some((partitionOffset, seqNr)) if event.seqNr <= seqNr =>
            val fork = JournalFork(
              key = key,
              detectedAtOffset = event.partitionOffset,
              detectedAtSeqNr = event.seqNr,
              conflictsWithOffset = partitionOffset,
              conflictsWithSeqNr = seqNr,
              origin = event.origin,
            )
            (state, fork :: forks)
          case _ =>
            (Some((event.partitionOffset, event.seqNr)), forks)
        }
    }

    forks.reverse
  }
}
