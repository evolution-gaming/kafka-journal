package com.evolution.kafka.journal.eventual.cassandra

import cats.syntax.all.*
import com.evolution.kafka.journal.*
import com.evolution.kafka.journal.eventual.cassandra.JournalFork.Consequence.{
  BelowDeleteTo,
  BreaksRecovery,
  SuspectedRegression,
}
import com.evolutiongaming.skafka.{Offset, Partition}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant

/**
 * [[JournalFork.fromEvents]] in isolation. The same detection is also exercised end to end, against
 * the whole append, in [[ReplicatedCassandraTest]].
 */
class JournalForkTest extends AnyFunSuite with Matchers {

  private val timestamp = Instant.parse("2019-12-12T10:10:10.00Z")
  private val journalKey = Key(id = "id", topic = "topic")
  private val origin = Origin("origin")

  private def event(seqNr: Int, offset: Int) = {
    EventRecord(
      event = Event[Unit](SeqNr.unsafe(seqNr)),
      timestamp = timestamp,
      partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(offset.toLong)),
      origin = origin.some,
      version = none,
      metadata = RecordMetadata.empty,
      headers = Headers.empty,
    )
  }

  private def journalHead(seqNr: SeqNr, offset: Int, deleteTo: Option[SeqNr] = none) = {
    JournalHead(
      partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(offset.toLong)),
      segmentSize = SegmentSize.default,
      seqNr = seqNr,
      deleteTo = deleteTo.map(_.toDeleteTo),
    ).some
  }

  private def journalHead(seqNr: Int, offset: Int): Option[JournalHead] = {
    journalHead(SeqNr.unsafe(seqNr), offset)
  }

  private def journalHead(seqNr: Int, offset: Int, deleteTo: Int): Option[JournalHead] = {
    journalHead(SeqNr.unsafe(seqNr), offset, SeqNr.unsafe(deleteTo).some)
  }

  test("no events, no forks") {
    val forks = JournalFork.fromEvents(
      journalKey,
      journalHead(seqNr = 1, offset = 1),
      List.empty[EventRecord[Unit]],
    )

    forks shouldEqual List.empty
  }

  test("strictly increasing seqNrs are not forks") {
    val forks1 = JournalFork.fromEvents(
      journalKey,
      none,
      List(event(seqNr = 1, offset = 1), event(seqNr = 2, offset = 2)),
    )
    val forks2 = JournalFork.fromEvents(
      journalKey,
      journalHead(seqNr = 2, offset = 2),
      List(event(seqNr = 3, offset = 3), event(seqNr = 4, offset = 4)),
    )

    forks1 shouldEqual List.empty
    forks2 shouldEqual List.empty
  }

  test("a seqNr gap is not a fork") {
    val forks = JournalFork.fromEvents(
      journalKey,
      journalHead(seqNr = 2, offset = 2),
      List(event(seqNr = 9, offset = 3)),
    )

    forks shouldEqual List.empty
  }

  test("repeating the journal head seqNr proves a duplicate") {
    val forks = JournalFork.fromEvents(
      journalKey,
      journalHead(seqNr = 2, offset = 2),
      List(event(seqNr = 2, offset = 3)),
    )

    forks.map(_.seqNr.value) shouldEqual List(2L)
    forks.map(_.consequence) shouldEqual List(BreaksRecovery)
  }

  test("a bare regression is only suspected - concurrent appends of distinct seqNrs look the same") {
    // what `JournalPerfSpec.appendNoise` does: distinct seqNrs appended to one key in parallel, so
    // they arrive out of order without any of them being written twice
    val forks1 = JournalFork.fromEvents(
      journalKey,
      journalHead(seqNr = 5, offset = 5),
      List(event(seqNr = 2, offset = 6)),
    )
    val forks2 = JournalFork.fromEvents(
      journalKey,
      none,
      List(event(seqNr = 3, offset = 1), event(seqNr = 1, offset = 2), event(seqNr = 2, offset = 3)),
    )

    forks1.map(_.consequence) shouldEqual List(SuspectedRegression)
    forks2.map(_.consequence) shouldEqual List(SuspectedRegression, SuspectedRegression)
  }

  test("every fork of a batch is reported, in event order") {
    val forks = JournalFork.fromEvents(
      journalKey,
      journalHead(seqNr = 3, offset = 3),
      List(event(seqNr = 2, offset = 4), event(seqNr = 3, offset = 5), event(seqNr = 1, offset = 6)),
    )

    forks.map(_.seqNr.value) shouldEqual List(2L, 3L, 1L)
  }

  test("a fork does not advance the earlier seqNr") {
    // seqNr 4 follows the head, so it is not a fork itself, but it does become what the events after
    // it are compared against
    val forks = JournalFork.fromEvents(
      journalKey,
      journalHead(seqNr = 3, offset = 3),
      List(event(seqNr = 1, offset = 4), event(seqNr = 4, offset = 5), event(seqNr = 4, offset = 6)),
    )

    forks.map(_.seqNr.value) shouldEqual List(1L, 4L)
    forks.map(_.earlierRecord.seqNr.value) shouldEqual List(3L, 4L)
  }

  test("a fork within one batch is reported without a journal head") {
    val forks = JournalFork.fromEvents(
      journalKey,
      none,
      List(event(seqNr = 1, offset = 1), event(seqNr = 1, offset = 2)),
    )

    forks.map(_.consequence) shouldEqual List(BreaksRecovery)
  }

  test("equal seqNrs within one batch prove a duplicate even below the running maximum") {
    // both 1s are below the batch's largest seqNr, so comparing against that maximum alone would call
    // them mere regressions - they are compared against the seqNrs already seen instead
    val forks = JournalFork.fromEvents(
      journalKey,
      none,
      List(event(seqNr = 9, offset = 1), event(seqNr = 1, offset = 2), event(seqNr = 1, offset = 3)),
    )

    forks.map(_.consequence) shouldEqual List(SuspectedRegression, BreaksRecovery)
  }

  test("a duplicate at or below deleteTo is reported, but as harmless") {
    val head = journalHead(seqNr = 5, offset = 5, deleteTo = 3)
    val forks1 = JournalFork.fromEvents(journalKey, head, List(event(seqNr = 2, offset = 6)))
    val forks2 = JournalFork.fromEvents(journalKey, head, List(event(seqNr = 3, offset = 6)))
    val forks3 = JournalFork.fromEvents(journalKey, head, List(event(seqNr = 4, offset = 6)))
    val forks4 = JournalFork.fromEvents(journalKey, head, List(event(seqNr = 5, offset = 6)))

    // no recovery reads these - whatever they duplicate is gone - but it is the same bug, so counted
    forks1.map(_.consequence) shouldEqual List(BelowDeleteTo)
    forks2.map(_.consequence) shouldEqual List(BelowDeleteTo)
    // above `deleteTo` nothing is deleted, so proof of a duplicate decides
    forks3.map(_.consequence) shouldEqual List(SuspectedRegression)
    forks4.map(_.consequence) shouldEqual List(BreaksRecovery)
  }

  test("a delete-all of a journal with no metajournal entry does not blind detection") {
    // `ReplicatedCassandra.delete` stores an unclamped `deleteTo` when there is no head yet, so a
    // `deleteMessagesTo(Long.MaxValue)` leaves `deleteTo` at `SeqNr.max`. Forks are still reported
    // there, just as harmless - suppressing them would silence the journal for good.
    val forks = JournalFork.fromEvents(
      journalKey,
      journalHead(SeqNr.max, offset = 1, deleteTo = SeqNr.max.some),
      List(event(seqNr = 1, offset = 2), event(seqNr = 1, offset = 3)),
    )

    forks.map(_.consequence) shouldEqual List(BelowDeleteTo, BelowDeleteTo)
  }

  test("origins of both records are carried over where known") {
    val later = event(seqNr = 1, offset = 2).copy(origin = Origin("later").some)
    val earlier = event(seqNr = 1, offset = 1).copy(origin = Origin("earlier").some)

    val withinBatch = JournalFork.fromEvents(journalKey, none, List(earlier, later))
    val againstHead = JournalFork.fromEvents(journalKey, journalHead(seqNr = 1, offset = 1), List(later))

    withinBatch.map(_.laterRecord.origin) shouldEqual List(Origin("later").some)
    withinBatch.map(_.earlierRecord.origin) shouldEqual List(Origin("earlier").some)

    // against the journal head the origin of the earlier writer is not at hand, see
    // `JournalFork.Record.fromJournalHead`
    againstHead.map(_.earlierRecord.origin) shouldEqual List(none)
  }
}
