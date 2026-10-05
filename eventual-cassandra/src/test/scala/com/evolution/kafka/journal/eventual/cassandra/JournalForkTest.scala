package com.evolution.kafka.journal.eventual.cassandra

import cats.syntax.all.*
import com.evolution.kafka.journal.*
import com.evolutiongaming.skafka.{Offset, Partition}
import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant

class JournalForkTest extends AnyFunSuite {

  private val key = Key(id = "id", topic = "topic")

  private def partitionOffset(offset: Int) = PartitionOffset(Partition.min, Offset.unsafe(offset))

  private def event(seqNr: Long, offset: Int, origin: Origin = Origin("origin")) = {
    EventRecord(
      event = Event[Unit](SeqNr.unsafe(seqNr)),
      timestamp = Instant.parse("2019-12-12T10:10:10.00Z"),
      partitionOffset = partitionOffset(offset),
      origin = Some(origin),
      version = none,
      metadata = RecordMetadata.empty,
      headers = Headers.empty,
    )
  }

  private def journalHead(seqNr: Long, offset: Int) = {
    Some(
      JournalHead(
        partitionOffset = partitionOffset(offset),
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

  test("repeating the journal head seqNr is a duplicate") {
    val events = List(event(seqNr = 2, offset = 3))
    val forks = JournalFork.fromEvents(key, journalHead(seqNr = 2, offset = 2), events)

    val fork = JournalFork(
      key = key,
      detectedAtOffset = partitionOffset(3),
      detectedAtSeqNr = SeqNr.unsafe(2),
      conflictsWithOffset = partitionOffset(2),
      conflictsWithSeqNr = SeqNr.unsafe(2),
      origin = Some(Origin("origin")),
    )
    assert(forks == List(fork))
    assert(forks.map(_.isDuplicate) == List(true))
  }

  test("a lower seqNr is reported as out of order") {
    val events1 = List(event(seqNr = 2, offset = 6))
    val events2 = List(event(seqNr = 3, offset = 1), event(seqNr = 1, offset = 2), event(seqNr = 2, offset = 3))
    val forks1 = JournalFork.fromEvents(key, journalHead(seqNr = 5, offset = 5), events1)
    val forks2 = JournalFork.fromEvents(key, none, events2)

    assert(forks1.map(_.isDuplicate) == List(false))
    assert(forks2.map(_.isDuplicate) == List(false, false))
  }

  test("every fork of a batch is reported, in event order") {
    val events = List(event(seqNr = 2, offset = 4), event(seqNr = 3, offset = 5), event(seqNr = 1, offset = 6))
    val forks = JournalFork.fromEvents(key, journalHead(seqNr = 3, offset = 3), events)

    assert(forks.map(_.detectedAtSeqNr.value) == List(2L, 3L, 1L))
  }

  test("only a higher seqNr changes what the next events are compared with") {
    val events = List(
      event(seqNr = 2, offset = 6),
      event(seqNr = 3, offset = 7),
      event(seqNr = 7, offset = 8),
      event(seqNr = 7, offset = 9),
    )
    val forks = JournalFork.fromEvents(key, journalHead(seqNr = 5, offset = 5), events)

    assert(forks.map(_.detectedAtSeqNr.value) == List(2L, 3L, 7L))
    assert(forks.map(_.conflictsWithSeqNr.value) == List(5L, 5L, 7L))
    assert(forks.map(_.conflictsWithOffset) == List(partitionOffset(5), partitionOffset(5), partitionOffset(8)))
  }

  test("a fork within one batch is reported without a journal head") {
    val events = List(event(seqNr = 1, offset = 1), event(seqNr = 1, offset = 2))
    val forks = JournalFork.fromEvents(key, none, events)

    assert(forks.map(_.isDuplicate) == List(true))
  }

  test("a repeated seqNr below the highest one is reported as out of order") {
    val events = List(event(seqNr = 9, offset = 1), event(seqNr = 1, offset = 2), event(seqNr = 1, offset = 3))
    val forks = JournalFork.fromEvents(key, none, events)

    assert(forks.map(_.isDuplicate) == List(false, false))
  }

  test("the origin is the one of the detected event") {
    val earlier = event(seqNr = 1, offset = 1, origin = Origin("earlier"))
    val later = event(seqNr = 1, offset = 2, origin = Origin("later"))
    val forks = JournalFork.fromEvents(key, none, List(earlier, later))

    assert(forks.map(_.origin) == List(Some(Origin("later"))))
  }

  test("events of one Kafka record are not forks of each other") {
    val events = List(event(seqNr = 1, offset = 1), event(seqNr = 2, offset = 1), event(seqNr = 3, offset = 1))
    val forks = JournalFork.fromEvents(key, none, events)

    assert(forks.isEmpty)
  }

  test("a stale Kafka record of several events is reported once per event, all at its offset") {
    val events = List(event(seqNr = 6, offset = 6), event(seqNr = 7, offset = 6), event(seqNr = 8, offset = 6))
    val forks = JournalFork.fromEvents(key, journalHead(seqNr = 8, offset = 5), events)

    assert(forks.map(_.detectedAtSeqNr.value) == List(6L, 7L, 8L))
    assert(forks.map(_.detectedAtOffset) == List(partitionOffset(6), partitionOffset(6), partitionOffset(6)))
    assert(forks.map(_.conflictsWithOffset) == List(partitionOffset(5), partitionOffset(5), partitionOffset(5)))
    assert(forks.map(_.isDuplicate) == List(false, false, true))
  }

  test("two Kafka records of one batch can conflict with each other") {
    val events = List(
      event(seqNr = 1, offset = 1),
      event(seqNr = 2, offset = 1),
      event(seqNr = 1, offset = 2),
      event(seqNr = 2, offset = 2),
    )
    val forks = JournalFork.fromEvents(key, none, events)

    assert(forks.map(_.detectedAtOffset) == List(partitionOffset(2), partitionOffset(2)))
    assert(forks.map(_.conflictsWithOffset) == List(partitionOffset(1), partitionOffset(1)))
    assert(forks.map(_.isDuplicate) == List(false, true))
  }
}
