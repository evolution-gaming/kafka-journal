package com.evolution.kafka.journal.eventual.cassandra

import cats.syntax.all.*
import com.evolution.kafka.journal.{EventRecord, Key, Origin, PartitionOffset, SeqNr}

/**
 * A possible journal fork: an event with a repeated or out-of-order `seqNr`, compared to the
 * journal head and to the earlier events of the same batch.
 *
 * Happens when an entity is restarted on another node while its previous instance still has an
 * append to Kafka in flight: the new instance does not see that event, and appends a different one
 * with the same `seqNr`. As `(seq_nr, timestamp)` is the clustering key of the `journal` table,
 * both events are stored, and the next recovery of the entity may fail with `Data integrity
 * violated: seqNr ... duplicated in multiple records`, see
 * [[com.evolution.kafka.journal.eventual.cassandra.EventualCassandra]].
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
   * True if the event repeats the `seqNr` of the record it is compared with. False if its `seqNr`
   * is lower: it may still repeat an earlier `seqNr`, but concurrent appends of distinct `seqNr`s
   * to one key look the same.
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
   * @param events
   *   the events about to be appended, in Kafka offset order, without the ones at or below the
   *   offset of `journalHead`: those were replicated already, and delivering them again is not a
   *   fork
   */
  def fromEvents[A](
    key: Key,
    journalHead: Option[JournalHead],
    events: List[EventRecord[A]],
  ): List[JournalFork] = {

    val state0 = journalHead.map { journalHead => (journalHead.partitionOffset, journalHead.seqNr) }
    val forks0 = List.empty[JournalFork]

    val (_, forks) = events.foldLeft((state0, forks0)) {
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
