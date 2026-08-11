package com.evolution.kafka.journal.eventual.cassandra

import cats.syntax.all.*
import com.evolution.kafka.journal.{EventRecord, Key, Origin, PartitionOffset, SeqNr}

/**
 * A candidate journal fork: an event whose `seqNr` is not above every `seqNr` replicated to that
 * journal before it, which the `seqNr`s of a journal are supposed to be.
 *
 * Happens when a persistent actor's append to Kafka is still in flight while the entity is
 * restarted elsewhere: the new incarnation does not see the in-flight event, so it appends a
 * different event with the same `seqNr`. Both are replicated (the `journal` table clusters on
 * `(seq_nr, timestamp)`), and a recovery of the entity can then fail with `Data integrity violated:
 * seqNr ... duplicated in multiple records`, see
 * [[com.evolution.kafka.journal.eventual.cassandra.EventualCassandra]].
 *
 * Detected, but not acted upon, and deliberately says nothing about what it costs: that depends on
 * how the journal is used, which is not known here. Which of the two branches survives is a
 * decision for a human or a repair tool, as it depends on the `writerUuid` of the events which
 * follow.
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
 *   event, see [[JournalFork.Record.fromJournalHead]], or an earlier event of the same batch.
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
  duplicateProven: Boolean,
) {

  def seqNr: SeqNr = laterRecord.seqNr

  def show: String = {
    s"key: $key, later: ${ laterRecord.show }, earlier: ${ earlierRecord.show }"
  }
}

private[journal] object JournalFork {

  /**
   * The forks among `events`, in the order the events appear.
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
   * Where one of a fork's two records sits in Kafka, and which node appended it.
   *
   * Notably not the `writerUuid` of the entity incarnation behind it, which is what decides the
   * surviving branch - that lives in the payload and is left to the repair tool.
   */
  final case class Record(seqNr: SeqNr, partitionOffset: PartitionOffset, origin: Option[Origin]) {

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
