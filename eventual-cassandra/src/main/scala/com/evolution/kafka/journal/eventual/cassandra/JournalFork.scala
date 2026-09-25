package com.evolution.kafka.journal.eventual.cassandra

import cats.syntax.all.*
import com.evolution.kafka.journal.{EventRecord, Key, Origin, PartitionOffset, SeqNr}

/**
 * A possible journal fork: an event whose `seqNr` is not greater than the highest `seqNr`
 * replicated to the journal before it.
 *
 * Happens when an entity is restarted on another node while its previous instance still has an
 * append to Kafka in flight: the new instance does not see that event, and appends a different one
 * with the same `seqNr`. As `(seq_nr, timestamp)` is the clustering key of the `journal` table,
 * both events are stored, and the next recovery of the entity may fail with `Data integrity
 * violated: seqNr ... duplicated in multiple records`, see
 * [[com.evolution.kafka.journal.eventual.cassandra.EventualCassandra]].
 *
 * @param laterRecord
 *   the event whose `seqNr` failed to increase
 * @param earlierRecord
 *   the record with the highest `seqNr` before `laterRecord`: either the last event replicated to
 *   the journal, see [[JournalFork.Record.fromJournalHead]], or an earlier event of the same batch
 * @param duplicateProven
 *   true if another record with the same `seqNr` is known to exist: the last replicated event, or
 *   an earlier event of the same batch. False if the `seqNr` only went down, which also happens
 *   when distinct `seqNr`s of one key are appended concurrently.
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

    val earlierRecord0 = journalHead.map(Record.fromJournalHead)
    val occupiedNrs0 = journalHead.map(_.seqNr).toSet
    val forks0 = List.empty[JournalFork]

    val (_, _, forks) = events.foldLeft((earlierRecord0, occupiedNrs0, forks0)) {
      case ((earlierRecord, occupiedNrs, forks), event) =>
        val laterRecord = Record.fromEventRecord(event)
        val occupiedNrs1 = occupiedNrs + laterRecord.seqNr
        earlierRecord match {
          case Some(earlierRecord) if laterRecord.seqNr <= earlierRecord.seqNr =>
            val fork = JournalFork(
              key = key,
              laterRecord = laterRecord,
              earlierRecord = earlierRecord,
              duplicateProven = occupiedNrs.contains(laterRecord.seqNr),
            )
            (Some(earlierRecord), occupiedNrs1, fork :: forks)
          case _ =>
            (Some(laterRecord), occupiedNrs1, forks)
        }
    }

    forks.reverse
  }

  /**
   * Where one of a fork's two records sits in Kafka, and which node appended it.
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
     * The last event replicated to the journal, built from its `metajournal` entry to avoid reading
     * the `journal` table. So it is less precise than [[fromEventRecord]]: `partitionOffset` is the
     * offset of the last Kafka record replicated for the journal, not necessarily of this event,
     * and `origin` is not set, because `metajournal` stores the origin of the writer which created
     * the entry, not of the last one.
     */
    def fromJournalHead(journalHead: JournalHead): Record = {
      Record(journalHead.seqNr, journalHead.partitionOffset, none)
    }
  }
}
