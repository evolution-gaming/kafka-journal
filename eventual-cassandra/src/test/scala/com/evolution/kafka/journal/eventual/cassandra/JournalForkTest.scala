package com.evolution.kafka.journal.eventual.cassandra

import cats.syntax.all.*
import com.evolution.kafka.journal.*
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

  private def eventOf(seqNr: Int, offset: Int) = {
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

  private def journalHeadOf(seqNr: Int, offset: Int, deleteTo: Option[Int] = none) = {
    JournalHead(
      partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(offset.toLong)),
      segmentSize = SegmentSize.default,
      seqNr = SeqNr.unsafe(seqNr),
      deleteTo = deleteTo.map { deleteTo => SeqNr.unsafe(deleteTo).toDeleteTo },
    ).some
  }

  private def forksOf(journalHead: Option[JournalHead], events: EventRecord[Unit]*) = {
    JournalFork
      .fromEvents(journalKey, journalHead, events.toList)
      .map { fork => (fork.seqNr.value, fork.earlierRecord.seqNr.value) }
  }

  private def consequencesOf(journalHead: Option[JournalHead], events: EventRecord[Unit]*) = {
    JournalFork
      .fromEvents(journalKey, journalHead, events.toList)
      .map { fork => (fork.seqNr.value, fork.consequence.name) }
  }

  test("no events, no forks") {
    forksOf(journalHeadOf(seqNr = 1, offset = 1)) shouldEqual List.empty
  }

  test("strictly increasing seqNrs are not forks") {
    forksOf(none, eventOf(seqNr = 1, offset = 1), eventOf(seqNr = 2, offset = 2)) shouldEqual List.empty
    forksOf(
      journalHeadOf(seqNr = 2, offset = 2),
      eventOf(seqNr = 3, offset = 3),
      eventOf(seqNr = 4, offset = 4),
    ) shouldEqual List.empty
  }

  test("a seqNr gap is not a fork") {
    forksOf(journalHeadOf(seqNr = 2, offset = 2), eventOf(seqNr = 9, offset = 3)) shouldEqual List.empty
  }

  test("a seqNr at or below the journal head is a fork") {
    forksOf(journalHeadOf(seqNr = 2, offset = 2), eventOf(seqNr = 2, offset = 3)) shouldEqual List((2L, 2L))
    forksOf(journalHeadOf(seqNr = 2, offset = 2), eventOf(seqNr = 1, offset = 3)) shouldEqual List((1L, 2L))
  }

  test("every fork of a batch is reported, in event order") {
    forksOf(
      journalHeadOf(seqNr = 3, offset = 3),
      eventOf(seqNr = 2, offset = 4),
      eventOf(seqNr = 3, offset = 5),
      eventOf(seqNr = 1, offset = 6),
    ) shouldEqual List((2L, 3L), (3L, 3L), (1L, 3L))
  }

  test("a fork does not advance the earlier seqNr") {
    // seqNr 4 follows the head, so it is not a fork itself, but it does become what the events after
    // it are compared against
    forksOf(
      journalHeadOf(seqNr = 3, offset = 3),
      eventOf(seqNr = 1, offset = 4),
      eventOf(seqNr = 4, offset = 5),
      eventOf(seqNr = 4, offset = 6),
    ) shouldEqual List((1L, 3L), (4L, 4L))
  }

  test("a fork within one batch is reported without a journal head") {
    forksOf(none, eventOf(seqNr = 1, offset = 1), eventOf(seqNr = 1, offset = 2)) shouldEqual List((1L, 1L))
  }

  test("repeating the journal head seqNr proves a duplicate") {
    consequencesOf(journalHeadOf(seqNr = 2, offset = 2), eventOf(seqNr = 2, offset = 3)) shouldEqual
      List((2L, "breaks_recovery"))
  }

  test("a bare regression is only suspected - concurrent appends of distinct seqNrs look the same") {
    // what `JournalPerfSpec.appendNoise` does: distinct seqNrs appended to one key in parallel, so
    // they arrive out of order without any of them being written twice
    consequencesOf(journalHeadOf(seqNr = 5, offset = 5), eventOf(seqNr = 2, offset = 6)) shouldEqual
      List((2L, "suspected_regression"))
    consequencesOf(
      none,
      eventOf(seqNr = 3, offset = 1),
      eventOf(seqNr = 1, offset = 2),
      eventOf(seqNr = 2, offset = 3),
    ) shouldEqual List((1L, "suspected_regression"), (2L, "suspected_regression"))
  }

  test("equal seqNrs within one batch prove a duplicate even below the running maximum") {
    // both 1s are below the batch's highest seqNr, so comparing against that maximum alone would
    // call them mere regressions - they are compared against the seqNrs already seen instead
    consequencesOf(
      none,
      eventOf(seqNr = 9, offset = 1),
      eventOf(seqNr = 1, offset = 2),
      eventOf(seqNr = 1, offset = 3),
    ) shouldEqual List((1L, "suspected_regression"), (1L, "breaks_recovery"))
  }

  test("a duplicate at or below deleteTo is reported, but as harmless") {
    def fork(seqNr: Int) = {
      consequencesOf(journalHeadOf(seqNr = 5, offset = 5, deleteTo = 3.some), eventOf(seqNr = seqNr, offset = 6))
    }

    // no recovery reads these - whatever they duplicate is gone - but it is the same bug, so counted
    fork(2) shouldEqual List((2L, "below_delete_to"))
    fork(3) shouldEqual List((3L, "below_delete_to"))
    // above `deleteTo` nothing is deleted, so proof of a duplicate decides
    fork(4) shouldEqual List((4L, "suspected_regression"))
    fork(5) shouldEqual List((5L, "breaks_recovery"))
  }

  test("a delete-all of a journal with no metajournal entry does not blind detection") {
    // `ReplicatedCassandra.delete` stores an unclamped `deleteTo` when there is no head yet, so a
    // `deleteMessagesTo(Long.MaxValue)` leaves `deleteTo` at `SeqNr.max`. Forks are still reported
    // there, just as harmless - suppressing them would silence the journal for good.
    val head = JournalHead(
      partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(1)),
      segmentSize = SegmentSize.default,
      seqNr = SeqNr.max,
      deleteTo = SeqNr.max.toDeleteTo.some,
    ).some

    consequencesOf(head, eventOf(seqNr = 1, offset = 2), eventOf(seqNr = 1, offset = 3)) shouldEqual
      List((1L, "below_delete_to"), (1L, "below_delete_to"))
  }

  test("origins of both records are carried over where known") {
    val later = eventOf(seqNr = 1, offset = 2).copy(origin = Origin("later").some)
    val earlier = eventOf(seqNr = 1, offset = 1).copy(origin = Origin("earlier").some)

    val withinBatch = JournalFork.fromEvents(journalKey, none, List(earlier, later))
    withinBatch.map { _.laterRecord.origin } shouldEqual List(Origin("later").some)
    withinBatch.map { _.earlierRecord.origin } shouldEqual List(Origin("earlier").some)

    // against the journal head the origin of the earlier writer is not at hand, see
    // `JournalFork.Record.fromJournalHead`
    val againstHead = JournalFork.fromEvents(journalKey, journalHeadOf(seqNr = 1, offset = 1), List(later))
    againstHead.map { _.earlierRecord.origin } shouldEqual List(none)
  }
}
