package com.evolution.kafka.journal.eventual.cassandra

import cats.syntax.all.*
import com.evolution.kafka.journal.{DeleteTo, EventRecord, Key, Origin, PartitionOffset, SeqNr}

/**
 * A candidate journal fork: an event whose `seqNr` is not above every `seqNr` replicated to that
 * journal before it, which the `seqNr`s of a journal are supposed to be.
 *
 * Happens when a persistent actor's append to Kafka is still in flight while the entity is
 * restarted elsewhere: the new incarnation does not see the in-flight event, so it appends a
 * different event with the same `seqNr`. Both are replicated (the `journal` table clusters on
 * `(seq_nr, timestamp)`), and the next recovery of the entity fails with `Data integrity violated:
 * seqNr ... duplicated in multiple records`, see
 * [[com.evolution.kafka.journal.eventual.cassandra.EventualCassandra]].
 *
 * Whether the `seqNr` really is written twice is only sometimes decidable here, see
 * [[JournalFork.Consequence]]: a recovery fails on two rows sharing a `seq_nr`, and knowing which
 * `seqNr`s a journal already occupies would take a `journal` table read this deliberately avoids.
 *
 * Detected, but not acted upon: which of the two branches survives is a decision for a human or a
 * repair tool, as it depends on the `writerUuid` of the events which follow the fork.
 *
 * The two records are named by their Kafka offset order, because that is all they always have in
 * common: `earlierRecord` may be the journal's last replicated event, but it may equally be another
 * event of the same batch, still being appended alongside `laterRecord`.
 *
 * @param key
 *   the journal
 * @param laterRecord
 *   the event being appended right now, the one whose `seqNr` failed to increase
 * @param earlierRecord
 *   the record with the highest `seqNr` at a lower offset - either the journal's last replicated
 *   event, see [[JournalFork.Record.fromJournalHead]], or an earlier event of this very batch,
 *   which is not in Cassandra yet at this point. This is the invariant that was violated; it is the
 *   record actually duplicated only when `duplicateProven` was established by comparing against it.
 * @param deleteTo
 *   the journal's delete watermark at the time
 * @param duplicateProven
 *   whether a record is known to occupy `seqNr` already - true when `laterRecord` repeats the
 *   `seqNr` of the journal head or of an earlier event of the same batch. False means the `seqNr`
 *   merely regressed: a duplicate is likely, but concurrent appends of *distinct* `seqNr`s to one
 *   key regress too, so it is not proof.
 */
private[journal] final case class JournalFork(
  key: Key,
  laterRecord: JournalFork.Record,
  earlierRecord: JournalFork.Record,
  deleteTo: Option[DeleteTo],
  duplicateProven: Boolean,
) {

  /**
   * The `seqNr` at stake, i.e. the one which failed to increase.
   */
  def seqNr: SeqNr = laterRecord.seqNr

  /**
   * What this means for the journal, see [[JournalFork.Consequence]]. A `seqNr` at or below
   * `deleteTo` is harmless whether it is duplicated or not: the record it would duplicate is gone
   * from the `journal` table and [[EventualCassandra]] starts reading above `deleteTo`.
   */
  def consequence: JournalFork.Consequence = {
    if (deleteTo.exists(_.value >= seqNr)) JournalFork.Consequence.BelowDeleteTo
    else if (duplicateProven) JournalFork.Consequence.BreaksRecovery
    else JournalFork.Consequence.SuspectedRegression
  }

  /**
   * The `seqNr ... duplicated` wording of the recovery side error is repeated on purpose, so that a
   * single grep finds both the fork and the recoveries it later breaks - but only where the
   * duplicate is proven, so that the phrase keeps meaning what it says.
   */
  def show: String = {
    val what =
      if (duplicateProven) s"Data integrity violated: seqNr $seqNr duplicated by a journal fork"
      else s"Suspected journal fork: seqNr $seqNr did not increase"
    s"$what, key: $key, later: ${ laterRecord.show }, " +
      s"earlier: ${ earlierRecord.show }, consequence: ${ consequence.explanation }"
  }
}

private[journal] object JournalFork {

  /**
   * The forks among `events`, in the order the events appear.
   *
   * The `seqNr`s of a journal strictly increase, so an event claiming one at or below the `seqNr`
   * of a record ahead of it - the last one replicated per `journalHead`, or an earlier event of
   * `events` - means one `seqNr` written twice.
   *
   * @param events
   *   the events about to be appended, in Kafka offset order, already filtered down to the ones not
   *   covered by `journalHead`'s offset - a re-delivered batch is not a fork
   */
  def fromEvents[A](
    key: Key,
    journalHead: Option[JournalHead],
    events: List[EventRecord[A]],
  ): List[JournalFork] = {

    val deleteTo = journalHead.flatMap(_.deleteTo)
    val headSeqNr = journalHead.map(_.seqNr)

    // `seqNr`s of the batch walked so far: needed on top of `earlier` because two *equal* `seqNr`s
    // both below the running maximum would otherwise never be compared to each other
    val (_, _, forks) = events.foldLeft(
      (journalHead.map(Record.fromJournalHead), Set.empty[SeqNr], List.empty[JournalFork]),
    ) {
      case ((earlier, seen, forks), event) =>
        val record = Record.fromEventRecord(event)
        val seen1 = seen + record.seqNr
        earlier match {
          case Some(earlier) if record.seqNr <= earlier.seqNr =>
            val fork = JournalFork(
              key,
              laterRecord = record,
              earlierRecord = earlier,
              deleteTo = deleteTo,
              duplicateProven = seen.contains(record.seqNr) || headSeqNr.contains(record.seqNr),
            )
            (Some(earlier), seen1, fork :: forks)
          case _ =>
            (Some(record), seen1, forks)
        }
    }

    forks.reverse
  }

  /**
   * What a [[JournalFork]] means for the journal, and hence how loudly to report it. The `name` is
   * a metric label value, so the set of them stays small and stable.
   */
  sealed abstract class Consequence(val name: String, val explanation: String)

  object Consequence {

    /**
     * A record is known to occupy the `seqNr` and is still readable, so the entity's next recovery
     * fails and stays broken until the losing row is removed by hand. Assumes the default
     * `seqNrUniqueness` data integrity setting on the reader side; with it off,
     * [[EventualCassandra]] replays both events instead of failing.
     */
    case object BreaksRecovery
    extends Consequence("breaks_recovery", "the next recovery of the entity will fail on it")

    /**
     * The `seqNr` is at or below the journal's `deleteTo`, so whatever it duplicates is already
     * gone and no recovery reads that far back. The appended row is orphaned until the next purge
     * or expiry - the same underlying bug, but nothing to repair.
     */
    case object BelowDeleteTo
    extends Consequence(
      "below_delete_to",
      "already deleted, so no recovery reads it - the appended row is merely orphaned",
    )

    /**
     * The `seqNr` regressed but nothing proves it is occupied: it is the fingerprint of the fork
     * bug, yet concurrent appends of distinct `seqNr`s to one key look identical from here.
     */
    case object SuspectedRegression
    extends Consequence(
      "suspected_regression",
      "a duplicate is likely but unproven - only a `journal` table read for this seqNr can tell",
    )
  }

  /**
   * As much of one of the two records claiming the same `seqNr` as is needed to find it again: the
   * [[EventRecord]] fields identifying where it sits in Kafka and which node appended it.
   *
   * Notably not the `writerUuid` of the entity incarnation behind it, which is what decides the
   * surviving branch - that lives in the payload and is left to the repair tool.
   */
  final case class Record(seqNr: SeqNr, partitionOffset: PartitionOffset, origin: Option[Origin]) {

    /**
     * Renders the record for [[JournalFork.show]].
     */
    def show: String = {
      val originStr = origin.foldMap { origin => s", origin: $origin" }
      s"seqNr: $seqNr, partition: ${ partitionOffset.partition }, offset: ${ partitionOffset.offset }$originStr"
    }
  }

  object Record {

    def fromEventRecord[A](event: EventRecord[A]): Record = {
      Record(event.seqNr, event.partitionOffset, event.origin)
    }

    /**
     * The last event replicated to a journal, as far as its `metajournal` entry tells without
     * reading the `journal` table: `partitionOffset` is the one of the entry as a whole rather than
     * of that single event, and `origin` is unknown - the one stored in `metajournal` belongs to
     * whoever created the entry, not to the last writer.
     */
    def fromJournalHead(journalHead: JournalHead): Record = {
      Record(journalHead.seqNr, journalHead.partitionOffset, none)
    }
  }
}
