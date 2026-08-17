package com.evolution.kafka.journal.eventual.cassandra

import cats.syntax.all.*
import com.evolution.kafka.journal.*
import com.evolutiongaming.skafka.{Offset, Partition}
import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant

/**
 * [[JournalFork.fromEvents]] in isolation. The same detection is also exercised end to end, against
 * the whole append, in [[ReplicatedCassandraTest]].
 */
class JournalForkTest extends AnyFunSuite {

  private val key = Key(id = "id", topic = "topic")

  private def event(seqNr: Long, offset: Int, origin: Origin = Origin("origin")) = {
    EventRecord(
      event = Event[Unit](SeqNr.unsafe(seqNr)),
      timestamp = Instant.parse("2019-12-12T10:10:10.00Z"),
      partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(offset)),
      origin = Some(origin),
      version = none,
      metadata = RecordMetadata.empty,
      headers = Headers.empty,
    )
  }

  private def journalHead(seqNr: Long, offset: Int) = {
    Some(
      JournalHead(
        partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(offset)),
        segmentSize = SegmentSize.default,
        seqNr = SeqNr.unsafe(seqNr),
      ),
    )
  }

  test("no events, no forks") {
    val events = List.empty[EventRecord[Unit]]
    val forks = JournalFork.fromEvents(key, journalHead(seqNr = 1, offset = 1), events)

    assert(forks.isEmpty)
  }

  test("strictly increasing seqNrs are not forks") {
    val events1 = List(event(seqNr = 1, offset = 1), event(seqNr = 2, offset = 2))
    val events2 = List(event(seqNr = 3, offset = 3), event(seqNr = 4, offset = 4))
    val forks1 = JournalFork.fromEvents(key, none, events1)
    val forks2 = JournalFork.fromEvents(key, journalHead(seqNr = 2, offset = 2), events2)

    assert(forks1.isEmpty)
    assert(forks2.isEmpty)
  }

  test("a seqNr gap is not a fork") {
    val events = List(event(seqNr = 9, offset = 3))
    val forks = JournalFork.fromEvents(key, journalHead(seqNr = 2, offset = 2), events)

    assert(forks.isEmpty)
  }

  test("repeating the journal head seqNr proves a duplicate") {
    val events = List(event(seqNr = 2, offset = 3))
    val forks = JournalFork.fromEvents(key, journalHead(seqNr = 2, offset = 2), events)

    assert(forks.map(_.seqNr.value) == List(2L))
    assert(forks.map(_.duplicateProven) == List(true))
  }

  test("a bare regression is only suspected - concurrent appends of distinct seqNrs look the same") {
    // what `JournalPerfSpec.appendNoise` does: distinct seqNrs appended to one key in parallel, so
    // they arrive out of order without any of them being written twice
    val events1 = List(event(seqNr = 2, offset = 6))
    val events2 = List(event(seqNr = 3, offset = 1), event(seqNr = 1, offset = 2), event(seqNr = 2, offset = 3))
    val forks1 = JournalFork.fromEvents(key, journalHead(seqNr = 5, offset = 5), events1)
    val forks2 = JournalFork.fromEvents(key, none, events2)

    assert(forks1.map(_.duplicateProven) == List(false))
    assert(forks2.map(_.duplicateProven) == List(false, false))
  }

  test("every fork of a batch is reported, in event order") {
    val events = List(event(seqNr = 2, offset = 4), event(seqNr = 3, offset = 5), event(seqNr = 1, offset = 6))
    val forks = JournalFork.fromEvents(key, journalHead(seqNr = 3, offset = 3), events)

    assert(forks.map(_.seqNr.value) == List(2L, 3L, 1L))
  }

  test("a fork does not advance the earlier seqNr") {
    // seqNr 4 follows the head, so it is not a fork itself, but it does become what the events after
    // it are compared against
    val events = List(event(seqNr = 1, offset = 4), event(seqNr = 4, offset = 5), event(seqNr = 4, offset = 6))
    val forks = JournalFork.fromEvents(key, journalHead(seqNr = 3, offset = 3), events)

    assert(forks.map(_.seqNr.value) == List(1L, 4L))
    assert(forks.map(_.earlierRecord.seqNr.value) == List(3L, 4L))
  }

  test("a fork within one batch is reported without a journal head") {
    val events = List(event(seqNr = 1, offset = 1), event(seqNr = 1, offset = 2))
    val forks = JournalFork.fromEvents(key, none, events)

    assert(forks.map(_.duplicateProven) == List(true))
  }

  test("equal seqNrs within one batch prove a duplicate even below the running maximum") {
    // both 1s are below the batch's largest seqNr, so comparing against that maximum alone would call
    // them mere regressions - they are compared against the seqNrs already seen instead
    val events = List(event(seqNr = 9, offset = 1), event(seqNr = 1, offset = 2), event(seqNr = 1, offset = 3))
    val forks = JournalFork.fromEvents(key, none, events)

    assert(forks.map(_.duplicateProven) == List(false, true))
  }

  test("origins of both records are carried over where known") {
    val later = event(seqNr = 1, offset = 2, origin = Origin("later"))
    val earlier = event(seqNr = 1, offset = 1, origin = Origin("earlier"))

    val withinBatch = JournalFork.fromEvents(key, none, List(earlier, later))
    val againstHead = JournalFork.fromEvents(key, journalHead(seqNr = 1, offset = 1), List(later))

    assert(withinBatch.map(_.laterRecord.origin) == List(Some(Origin("later"))))
    assert(withinBatch.map(_.earlierRecord.origin) == List(Some(Origin("earlier"))))

    // against the journal head the origin of the earlier writer is not at hand, see
    // `JournalFork.Record.fromJournalHead`
    assert(againstHead.map(_.earlierRecord.origin) == List(none))
  }
}
