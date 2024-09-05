package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.data.{IndexedStateT, NonEmptyList as Nel}
import cats.effect.kernel.CancelScope
import cats.effect.std.UUIDGen
import cats.effect.{Poll, Sync}
import cats.implicits.*
import cats.syntax.all.none
import cats.{Id, Parallel}
import com.evolutiongaming.catshelper.DataHelper.*
import com.evolutiongaming.kafka.journal.*
import com.evolutiongaming.kafka.journal.ExpireAfter.implicits.*
import com.evolutiongaming.kafka.journal.eventual.EventualPayloadAndType
import com.evolutiongaming.kafka.journal.eventual.cassandra.ExpireOn.implicits.*
import com.evolutiongaming.kafka.journal.util.SkafkaHelper.*
import com.evolutiongaming.kafka.journal.util.TemporalHelper.*
import com.evolutiongaming.kafka.journal.util.{Fail, MonadCancelFromMonadError}
import com.evolutiongaming.skafka.{Offset, Partition, Topic}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import play.api.libs.json.Json

import java.time.{Instant, LocalDate, ZoneOffset}
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.collection.immutable.SortedSet
import scala.concurrent.duration.*
import scala.util.Try

class ReplicatedCassandraTest extends AnyFunSuite with Matchers {
  import ReplicatedCassandraTest.*

  private val timestamp0      = Instant.parse("2019-12-12T10:10:10.00Z")
  private val timestamp1      = timestamp0 + 1.minute
  private val topic0          = "topic0"
  private val topic1          = "topic1"
  private val partitionOffset = PartitionOffset.empty
  private val origin          = Origin("origin")
  private val version         = Version.current
  private val correlationUUID = UUID.randomUUID()
  private val correlationId   = CorrelationId.unsafe(correlationUUID.toString)
  private val record          = eventRecordOf(SeqNr.min, partitionOffset)

  private def eventRecordOf(seqNr: SeqNr, partitionOffset: PartitionOffset) = {
    EventRecord(
      event           = Event[EventualPayloadAndType](seqNr),
      timestamp       = timestamp0,
      partitionOffset = partitionOffset,
      origin          = origin.some,
      version         = version.some,
      metadata        = RecordMetadata(HeaderMetadata(Json.obj(("key", "value")).some), PayloadMetadata.empty),
      headers         = Headers(("key", "value")),
    ).withCorrelationId(correlationId)
  }

  for {
    segmentSize <- List(SegmentSize.min, SegmentSize.default, SegmentSize.max)
    segments    <- List((Segments.min, Segments.old), (Segments.old, Segments.default))
  } {
    val (segmentsFirst, segmentsSecond) = segments
    val segmentNrsOf                    = SegmentNrsOf[StateT](first = segmentsFirst, second = segmentsSecond)
    val segmentOfId                     = SegmentOf[Id](segmentsFirst)
    val journal = {
      implicit val parallel = Parallel.identity[StateT]
      implicit val uuidGen = new UUIDGen[StateT] {
        override def randomUUID = correlationUUID.pure[StateT]
      }
      ReplicatedCassandra
        .apply1(
          segmentSize,
          segmentNrsOf,
          statements,
          ExpiryService(ZoneOffset.UTC),
        )
        .toFlat
    }

    val suffix = s"segmentSize: $segmentSize, segments: $segments"

    test(s"topics, $suffix") {
      val id      = "id"
      val key     = Key(id = id, topic = topic0)
      val segment = segmentOfId(key)
      val offset1 = partitionOffset.offset.inc[Try].get

      val stateT = for {
        topics <- journal.topics
        _       = topics shouldEqual Set.empty
        _      <- journal.append(key, partitionOffset.partition, partitionOffset.offset, timestamp0, none, Nel.of(record))
        topics <- journal.topics
        _       = topics shouldEqual Set.empty
        _      <- journal.offsetCreate(topic0, Partition.min, Offset.min, timestamp0)
        topics <- journal.topics
        _       = topics shouldEqual Set(topic0)
        _      <- journal.offsetUpdate(topic0, Partition.min, Offset.unsafe(1), timestamp1)
        topics <- journal.topics
        _       = topics shouldEqual Set(topic0)
        _      <- journal.offsetCreate(topic1, Partition.min, Offset.min, timestamp0)
        topics <- journal.topics
        _       = topics shouldEqual Set(topic0, topic1)
        _      <- journal.delete(key, partitionOffset.partition, offset1, timestamp1, SeqNr.max.toDeleteTo, origin.some)
        topics <- journal.topics
        _       = topics shouldEqual Set(topic0, topic1)
      } yield {}

      val expected = State(
        actions = List(
          Action.UpdateDeleteTo(key, segment, partitionOffset.copy(Partition.min, offset1), timestamp1, SeqNr.min.toDeleteTo),
          Action.InsertMetaJournal(
            key,
            segment,
            created = timestamp0,
            updated = timestamp0,
            JournalHead(partitionOffset, segmentSize, SeqNr.min, correlationId = correlationId.some),
            origin.some,
          ),
          Action.InsertRecords(key, SegmentNr.min, 1),
        ),
        pointers = Map(
          (topic0, Map((Partition.min, PointerEntry(Offset.unsafe(1), created = timestamp0, updated = timestamp1)))),
          (topic1, Map((Partition.min, PointerEntry(Offset.min, created = timestamp0, updated = timestamp0)))),
        ),
        metaJournal = Map(
          (
            (topic0, segment),
            Map(
              (
                id,
                MetaJournalEntry(
                  journalHead = JournalHead(
                    partitionOffset = partitionOffset.copy(offset = offset1),
                    segmentSize     = segmentSize,
                    seqNr           = SeqNr.min,
                    deleteTo        = SeqNr.min.toDeleteTo.some,
                    correlationId   = correlationId.some,
                  ),
                  created = timestamp0,
                  updated = timestamp1,
                  origin  = origin.some,
                ),
              ),
            ),
          ),
        ),
      )
      val result = stateT.run(State.empty)
      result shouldEqual (expected, ()).pure[Try]
    }

    test(s"offset, $suffix") {
      val id      = "id"
      val key     = Key(id = id, topic = topic0)
      val segment = segmentOfId(key)
      val stateT = for {
        offset <- journal.offset(topic0, Partition.min)
        _       = offset shouldEqual none
        _ <- journal.append(key, partitionOffset.partition, partitionOffset.offset, timestamp0, none[ExpireAfter], Nel.of(record))
        offset <- journal.offset(topic0, Partition.min)
        _       = offset shouldEqual none
        _      <- journal.offsetCreate(topic0, Partition.min, Offset.min, timestamp0)
        offset <- journal.offset(topic0, Partition.min)
        _       = offset shouldEqual Offset.min.some
        _      <- journal.offsetUpdate(topic0, Partition.min, Offset.unsafe(1), timestamp1)
        offset <- journal.offset(topic0, Partition.min)
        _       = offset shouldEqual Offset.unsafe(1).some
        _      <- journal.offsetCreate(topic1, Partition.min, Offset.min, timestamp0)
        offset <- journal.offset(topic0, Partition.min)
        _       = offset shouldEqual Offset.unsafe(1).some
      } yield {}

      val expected = State(
        actions = List(
          Action.InsertMetaJournal(
            key,
            segment,
            created = timestamp0,
            updated = timestamp0,
            JournalHead(partitionOffset, segmentSize, SeqNr.min, correlationId = correlationId.some),
            origin.some,
          ),
          Action.InsertRecords(key, SegmentNr.min, 1),
        ),
        pointers = Map(
          (topic0, Map((Partition.min, PointerEntry(Offset.unsafe(1), created = timestamp0, updated = timestamp1)))),
          (topic1, Map((Partition.min, PointerEntry(Offset.min, created = timestamp0, updated = timestamp0)))),
        ),
        metaJournal = Map(
          (
            (topic0, segment),
            Map(
              (
                id,
                MetaJournalEntry(
                  journalHead = JournalHead(
                    partitionOffset = partitionOffset,
                    segmentSize     = segmentSize,
                    seqNr           = SeqNr.min,
                    correlationId   = correlationId.some,
                  ),
                  created = timestamp0,
                  updated = timestamp0,
                  origin  = origin.some,
                ),
              ),
            ),
          ),
        ),
        journal = Map(((key, SegmentNr.min), Map(((SeqNr.min, timestamp0), record)))),
      )
      val result = stateT.run(State.empty)
      result shouldEqual (expected, ()).pure[Try]
    }

    test(s"append, $suffix") {
      val id0      = "id0"
      val id1      = "id1"
      val key0     = Key(id0, topic0)
      val key1     = Key(id1, topic1)
      val segment0 = segmentOfId(key0)
      val segment1 = segmentOfId(key1)
      val expiry   = Expiry(1.minute.toExpireAfter, LocalDate.of(2019, 12, 12).toExpireOn)
      val stateT = for {
        _ <- journal.append(
          key = key0,
          Partition.min,
          Offset.min,
          timestamp   = timestamp0,
          expireAfter = expiry.after.some,
          events = Nel.of(eventRecordOf(seqNr = SeqNr.unsafe(1), partitionOffset = PartitionOffset(Partition.min, Offset.min))),
        )
        _ <- journal.append(
          key = key0,
          Partition.min,
          Offset.unsafe(3),
          timestamp   = timestamp1,
          expireAfter = none,
          events = Nel.of(
            eventRecordOf(seqNr = SeqNr.unsafe(2), partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(1))),
            eventRecordOf(seqNr = SeqNr.unsafe(3), partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(2))),
          ),
        )
        _ <- journal.append(
          key = key1,
          Partition.min,
          Offset.unsafe(4),
          timestamp   = timestamp0,
          expireAfter = none,
          events =
            Nel.of(eventRecordOf(seqNr = SeqNr.unsafe(1), partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(4)))),
        )
      } yield {}

      val events0 = Nel
        .of(
          eventRecordOf(seqNr = SeqNr.unsafe(1), partitionOffset = PartitionOffset(Partition.min, Offset.min)),
          eventRecordOf(seqNr = SeqNr.unsafe(2), partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(1))),
          eventRecordOf(seqNr = SeqNr.unsafe(3), partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(2))),
        )
        .grouped(segmentSize.value)
        .zipWithIndex
        .map {
          case (events, segmentNr) =>
            val map = events
              .map { a => ((a.seqNr, a.timestamp), a) }
              .toList
              .toMap
            ((key0, SegmentNr.unsafe(segmentNr)), map)
        }
        .toList
        .toMap

      val actions = if (segmentSize <= SegmentSize.min) {
        List(
          Action.InsertMetaJournal(
            key1,
            segment1,
            created = timestamp0,
            updated = timestamp0,
            JournalHead(
              PartitionOffset(Partition.min, Offset.unsafe(4)),
              segmentSize,
              SeqNr.min,
              correlationId = correlationId.some,
            ),
            origin.some,
          ),
          Action.InsertRecords(key1, SegmentNr.min, 1),
          Action.DeleteExpiry(key0, segment0),
          Action.UpdateSeqNr(key0, segment0, PartitionOffset(Partition.min, Offset.unsafe(3)), timestamp1, SeqNr.unsafe(3)),
          Action.InsertRecords(key0, SegmentNr.unsafe(1), 1),
          Action.InsertRecords(key0, SegmentNr.min, 1),
          Action.InsertMetaJournal(
            key0,
            segment0,
            created = timestamp0,
            updated = timestamp0,
            JournalHead(partitionOffset, segmentSize, SeqNr.min, none, expiry.some, correlationId.some),
            origin.some,
          ),
          Action.InsertRecords(key0, SegmentNr.min, 1),
        )
      } else {
        List(
          Action.InsertMetaJournal(
            key1,
            segment1,
            created = timestamp0,
            updated = timestamp0,
            JournalHead(
              PartitionOffset(Partition.min, Offset.unsafe(4)),
              segmentSize,
              SeqNr.min,
              correlationId = correlationId.some,
            ),
            origin.some,
          ),
          Action.InsertRecords(key1, SegmentNr.min, 1),
          Action.DeleteExpiry(key0, segment0),
          Action.UpdateSeqNr(key0, segment0, PartitionOffset(Partition.min, Offset.unsafe(3)), timestamp1, SeqNr.unsafe(3)),
          Action.InsertRecords(key0, SegmentNr.min, 2),
          Action.InsertMetaJournal(
            key0,
            segment0,
            created = timestamp0,
            updated = timestamp0,
            JournalHead(partitionOffset, segmentSize, SeqNr.min, none, expiry.some, correlationId.some),
            origin.some,
          ),
          Action.InsertRecords(key0, SegmentNr.min, 1),
        )
      }

      val expected = State(
        actions = actions,
        metaJournal = Map(
          (
            (topic0, segment0),
            Map(
              (
                id0,
                MetaJournalEntry(
                  journalHead = JournalHead(
                    partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(3)),
                    segmentSize     = segmentSize,
                    seqNr           = SeqNr.unsafe(3),
                    deleteTo        = none,
                    expiry          = none,
                    correlationId   = correlationId.some,
                  ),
                  created = timestamp0,
                  updated = timestamp1,
                  origin  = origin.some,
                ),
              ),
            ),
          ),
          (
            (topic1, segment1),
            Map(
              (
                id1,
                MetaJournalEntry(
                  journalHead = JournalHead(
                    partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(4)),
                    segmentSize     = segmentSize,
                    seqNr           = SeqNr.unsafe(1),
                    correlationId   = correlationId.some,
                  ),
                  created = timestamp0,
                  updated = timestamp0,
                  origin  = origin.some,
                ),
              ),
            ),
          ),
        ),
        journal = events0 ++ Map(
          (
            (key1, SegmentNr.min),
            Map(
              (
                (SeqNr.min, timestamp0),
                eventRecordOf(seqNr = SeqNr.unsafe(1), partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(4))),
              ),
            ),
          ),
        ),
      )
      val result = stateT.run(State.empty)
      result shouldEqual (expected, ()).pure[Try]
    }

    test(s"append & override expireAfter, $suffix") {
      val id      = "id"
      val key     = Key(id, topic0)
      val segment = segmentOfId(key)
      val expiry0 = Expiry(1.minute.toExpireAfter, LocalDate.of(2019, 12, 12).toExpireOn)
      val expiry1 = Expiry(2.minute.toExpireAfter, LocalDate.of(2019, 12, 12).toExpireOn)
      val stateT = for {
        _ <- journal.append(
          key = key,
          Partition.min,
          Offset.min,
          timestamp   = timestamp0,
          expireAfter = expiry0.after.some,
          events = Nel.of(eventRecordOf(seqNr = SeqNr.unsafe(1), partitionOffset = PartitionOffset(Partition.min, Offset.min))),
        )
        _ <- journal.append(
          key = key,
          Partition.min,
          Offset.unsafe(3),
          timestamp   = timestamp1,
          expireAfter = expiry1.after.some,
          events = Nel.of(
            eventRecordOf(seqNr = SeqNr.unsafe(2), partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(1))),
            eventRecordOf(seqNr = SeqNr.unsafe(3), partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(2))),
          ),
        )
      } yield {}

      val events0 = Nel
        .of(
          eventRecordOf(seqNr = SeqNr.unsafe(1), partitionOffset = PartitionOffset(Partition.min, Offset.min)),
          eventRecordOf(seqNr = SeqNr.unsafe(2), partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(1))),
          eventRecordOf(seqNr = SeqNr.unsafe(3), partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(2))),
        )
        .grouped(segmentSize.value)
        .zipWithIndex
        .map {
          case (events, segmentNr) =>
            val map = events
              .map { a => ((a.seqNr, a.timestamp), a) }
              .toList
              .toMap
            ((key, SegmentNr.unsafe(segmentNr)), map)
        }
        .toList
        .toMap

      val actions = if (segmentSize <= SegmentSize.min) {
        List(
          Action
            .UpdateExpiry(key, segment, PartitionOffset(Partition.min, Offset.unsafe(3)), timestamp1, SeqNr.unsafe(3), expiry1),
          Action.InsertRecords(key, SegmentNr.unsafe(1), 1),
          Action.InsertRecords(key, SegmentNr.min, 1),
          Action.InsertMetaJournal(
            key,
            segment,
            created = timestamp0,
            updated = timestamp0,
            JournalHead(partitionOffset, segmentSize, SeqNr.min, none, expiry0.some, correlationId = correlationId.some),
            origin.some,
          ),
          Action.InsertRecords(key, SegmentNr.min, 1),
        )
      } else {
        List(
          Action
            .UpdateExpiry(key, segment, PartitionOffset(Partition.min, Offset.unsafe(3)), timestamp1, SeqNr.unsafe(3), expiry1),
          Action.InsertRecords(key, SegmentNr.min, 2),
          Action.InsertMetaJournal(
            key,
            segment,
            created = timestamp0,
            updated = timestamp0,
            JournalHead(partitionOffset, segmentSize, SeqNr.min, none, expiry0.some, correlationId = correlationId.some),
            origin.some,
          ),
          Action.InsertRecords(key, SegmentNr.min, 1),
        )
      }

      val expected = State(
        actions = actions,
        metaJournal = Map(
          (
            (topic0, segment),
            Map(
              (
                id,
                MetaJournalEntry(
                  journalHead = JournalHead(
                    partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(3)),
                    segmentSize     = segmentSize,
                    seqNr           = SeqNr.unsafe(3),
                    deleteTo        = none,
                    expiry          = expiry1.some,
                    correlationId   = correlationId.some,
                  ),
                  created = timestamp0,
                  updated = timestamp1,
                  origin  = origin.some,
                ),
              ),
            ),
          ),
        ),
        journal = events0,
      )
      val result = stateT.run(State.empty)
      result shouldEqual (expected, ()).pure[Try]
    }

    test(s"update expiry, $suffix") {
      val id  = "id"
      val key = Key(id = id, topic = topic0)

      val expiry0 = Expiry(1.minute.toExpireAfter, LocalDate.of(2019, 12, 12).toExpireOn)

      val expiry1 = Expiry(2.minutes.toExpireAfter, LocalDate.of(2019, 12, 12).toExpireOn)

      val segment = segmentOfId(key)
      val stateT = for {
        _ <- journal.append(
          key = key,
          Partition.min,
          Offset.unsafe(1),
          timestamp   = timestamp0,
          expireAfter = expiry0.after.some,
          events =
            Nel.of(eventRecordOf(seqNr = SeqNr.unsafe(1), partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(1)))),
        )
        _ <- journal.append(
          key = key,
          Partition.min,
          Offset.unsafe(2),
          timestamp   = timestamp1,
          expireAfter = expiry1.after.some,
          events =
            Nel.of(eventRecordOf(seqNr = SeqNr.unsafe(2), partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(2)))),
        )
      } yield {}

      val expected = State(
        actions = List(
          Action
            .UpdateExpiry(key, segment, PartitionOffset(Partition.min, Offset.unsafe(2)), timestamp1, SeqNr.unsafe(2), expiry1),
          Action.InsertRecords(key, SegmentNr.min, 1),
          Action.InsertMetaJournal(
            key,
            segment,
            created = timestamp0,
            updated = timestamp0,
            JournalHead(
              PartitionOffset(Partition.min, Offset.unsafe(1)),
              segmentSize,
              SeqNr.unsafe(1),
              none,
              expiry0.some,
              correlationId = correlationId.some,
            ),
            origin.some,
          ),
          Action.InsertRecords(key, SegmentNr.min, 1),
        ),
        metaJournal = Map(
          (
            (topic0, segment),
            Map(
              (
                id,
                MetaJournalEntry(
                  journalHead = JournalHead(
                    partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(2)),
                    segmentSize     = segmentSize,
                    seqNr           = SeqNr.unsafe(2),
                    deleteTo        = none,
                    expiry          = expiry1.some,
                    correlationId   = correlationId.some,
                  ),
                  created = timestamp0,
                  updated = timestamp1,
                  origin  = origin.some,
                ),
              ),
            ),
          ),
        ),
        journal = Map(
          (
            (key, SegmentNr.min),
            Map(
              (
                (SeqNr.unsafe(1), timestamp0),
                eventRecordOf(seqNr = SeqNr.unsafe(1), partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(1))),
              ),
              (
                (SeqNr.unsafe(2), timestamp0),
                eventRecordOf(seqNr = SeqNr.unsafe(2), partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(2))),
              ),
            ),
          ),
        ),
      )
      val result = stateT.run(State.empty)
      result shouldEqual (expected, ()).pure[Try]
    }

    test(s"not update expiry, $suffix") {
      val id  = "id"
      val key = Key(id = id, topic = topic0)

      val expiry = Expiry(1.minute.toExpireAfter, LocalDate.of(2019, 12, 12).toExpireOn)

      val segment = segmentOfId(key)
      val stateT = for {
        _ <- journal.append(
          key = key,
          Partition.min,
          Offset.unsafe(1),
          timestamp   = timestamp0,
          expireAfter = expiry.after.some,
          events =
            Nel.of(eventRecordOf(seqNr = SeqNr.unsafe(1), partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(1)))),
        )
        _ <- journal.append(
          key = key,
          Partition.min,
          Offset.unsafe(2),
          timestamp   = timestamp1,
          expireAfter = expiry.after.some,
          events =
            Nel.of(eventRecordOf(seqNr = SeqNr.unsafe(2), partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(2)))),
        )
      } yield {}

      val expected = State(
        actions = List(
          Action.UpdateSeqNr(key, segment, PartitionOffset(Partition.min, Offset.unsafe(2)), timestamp1, SeqNr.unsafe(2)),
          Action.InsertRecords(key, SegmentNr.min, 1),
          Action.InsertMetaJournal(
            key,
            segment,
            created = timestamp0,
            updated = timestamp0,
            JournalHead(
              PartitionOffset(Partition.min, Offset.unsafe(1)),
              segmentSize,
              SeqNr.unsafe(1),
              none,
              expiry.some,
              correlationId = correlationId.some,
            ),
            origin.some,
          ),
          Action.InsertRecords(key, SegmentNr.min, 1),
        ),
        metaJournal = Map(
          (
            (topic0, segment),
            Map(
              (
                id,
                MetaJournalEntry(
                  journalHead = JournalHead(
                    partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(2)),
                    segmentSize     = segmentSize,
                    seqNr           = SeqNr.unsafe(2),
                    deleteTo        = none,
                    expiry          = expiry.some,
                    correlationId   = correlationId.some,
                  ),
                  created = timestamp0,
                  updated = timestamp1,
                  origin  = origin.some,
                ),
              ),
            ),
          ),
        ),
        journal = Map(
          (
            (key, SegmentNr.min),
            Map(
              (
                (SeqNr.unsafe(1), timestamp0),
                eventRecordOf(seqNr = SeqNr.unsafe(1), partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(1))),
              ),
              (
                (SeqNr.unsafe(2), timestamp0),
                eventRecordOf(seqNr = SeqNr.unsafe(2), partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(2))),
              ),
            ),
          ),
        ),
      )
      val result = stateT.run(State.empty)
      result shouldEqual (expected, ()).pure[Try]
    }

    test(s"remove expiry, $suffix") {
      val id  = "id"
      val key = Key(id = id, topic = topic0)

      val expiry = Expiry(1.minute.toExpireAfter, LocalDate.of(2019, 12, 12).toExpireOn)

      val segment = segmentOfId(key)
      val stateT = for {
        _ <- journal.append(
          key = key,
          Partition.min,
          Offset.unsafe(1),
          timestamp   = timestamp0,
          expireAfter = expiry.after.some,
          events =
            Nel.of(eventRecordOf(seqNr = SeqNr.unsafe(1), partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(1)))),
        )
        _ <- journal.append(
          key = key,
          Partition.min,
          Offset.unsafe(2),
          timestamp   = timestamp1,
          expireAfter = none,
          events =
            Nel.of(eventRecordOf(seqNr = SeqNr.unsafe(2), partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(2)))),
        )
      } yield {}

      val expected = State(
        actions = List(
          Action.DeleteExpiry(key, segment),
          Action.UpdateSeqNr(key, segment, PartitionOffset(Partition.min, Offset.unsafe(2)), timestamp1, SeqNr.unsafe(2)),
          Action.InsertRecords(key, SegmentNr.min, 1),
          Action.InsertMetaJournal(
            key,
            segment,
            created = timestamp0,
            updated = timestamp0,
            JournalHead(
              PartitionOffset(Partition.min, Offset.unsafe(1)),
              segmentSize,
              SeqNr.unsafe(1),
              none,
              expiry.some,
              correlationId.some,
            ),
            origin.some,
          ),
          Action.InsertRecords(key, SegmentNr.min, 1),
        ),
        metaJournal = Map(
          (
            (topic0, segment),
            Map(
              (
                id,
                MetaJournalEntry(
                  journalHead = JournalHead(
                    partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(2)),
                    segmentSize     = segmentSize,
                    seqNr           = SeqNr.unsafe(2),
                    correlationId   = correlationId.some,
                  ),
                  created = timestamp0,
                  updated = timestamp1,
                  origin  = origin.some,
                ),
              ),
            ),
          ),
        ),
        journal = Map(
          (
            (key, SegmentNr.min),
            Map(
              (
                (SeqNr.unsafe(1), timestamp0),
                eventRecordOf(seqNr = SeqNr.unsafe(1), partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(1))),
              ),
              (
                (SeqNr.unsafe(2), timestamp0),
                eventRecordOf(seqNr = SeqNr.unsafe(2), partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(2))),
              ),
            ),
          ),
        ),
      )
      val result = stateT.run(State.empty)
      result shouldEqual (expected, ()).pure[Try]
    }

    test(s"not repeat appends, $suffix") {
      val id      = "id"
      val key     = Key(id, topic0)
      val segment = segmentOfId(key)
      val stateT = journal.append(
        key = key,
        Partition.min,
        Offset.unsafe(4),
        timestamp   = timestamp1,
        expireAfter = none,
        events = Nel.of(
          eventRecordOf(seqNr = SeqNr.unsafe(1), partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(1))),
          eventRecordOf(seqNr = SeqNr.unsafe(2), partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(3))),
        ),
      )

      val expected = State(
        actions = List(
          Action.UpdateSeqNr(key, segment, PartitionOffset(Partition.min, Offset.unsafe(4)), timestamp1, SeqNr.unsafe(2)),
          Action.InsertRecords(key, SegmentNr.min, 1),
        ),
        metaJournal = Map(
          (
            (topic0, segment),
            Map(
              (
                id,
                MetaJournalEntry(
                  journalHead = JournalHead(
                    partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(4)),
                    segmentSize     = segmentSize,
                    seqNr           = SeqNr.unsafe(2),
                    correlationId   = correlationId.some,
                  ),
                  created = timestamp0,
                  updated = timestamp1,
                  origin  = origin.some,
                ),
              ),
            ),
          ),
        ),
        journal = Map(
          (
            (key, SegmentNr.min),
            Map(
              (
                (SeqNr.unsafe(2), timestamp0),
                eventRecordOf(seqNr = SeqNr.unsafe(2), partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(3))),
              ),
            ),
          ),
        ),
      )

      val initial = State
        .empty
        .copy(
          metaJournal = Map(
            (
              (topic0, segment),
              Map(
                (
                  id,
                  MetaJournalEntry(
                    journalHead = JournalHead(
                      partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(2)),
                      segmentSize     = segmentSize,
                      seqNr           = SeqNr.unsafe(1),
                      correlationId   = correlationId.some,
                    ),
                    created = timestamp0,
                    updated = timestamp0,
                    origin  = origin.some,
                  ),
                ),
              ),
            ),
          ),
        )

      val actual = stateT.run(initial)
      actual shouldEqual (expected, true).pure[Try]
    }

    test(s"delete, $suffix") {
      val id      = "id"
      val key     = Key(id = id, topic = topic0)
      val segment = segmentOfId(key)
      val stateT = for {
        _ <- journal.append(
          key = key,
          Partition.min,
          Offset.unsafe(1),
          timestamp   = timestamp0,
          expireAfter = none,
          events      = Nel.of(eventRecordOf(seqNr = SeqNr.min, partitionOffset = PartitionOffset(Partition.min, Offset.min))),
        )
        _ <- journal.delete(
          key = key,
          Partition.min,
          Offset.unsafe(2),
          timestamp = timestamp1,
          deleteTo  = SeqNr.max.toDeleteTo,
          origin    = origin.some,
        )
        _ <- journal.delete(
          key = key,
          Partition.min,
          Offset.unsafe(3),
          timestamp = timestamp1,
          deleteTo  = SeqNr.max.toDeleteTo,
          origin    = origin.some,
        )
      } yield {}

      val expected = State(
        actions = List(
          Action.UpdatePartitionOffset(key, segment, partitionOffset.copy(Partition.min, Offset.unsafe(3)), timestamp1),
          Action.UpdateDeleteTo(
            key,
            segment,
            partitionOffset.copy(Partition.min, Offset.unsafe(2)),
            timestamp1,
            SeqNr.min.toDeleteTo,
          ),
          Action.InsertMetaJournal(
            key,
            segment,
            created = timestamp0,
            updated = timestamp0,
            JournalHead(
              PartitionOffset(Partition.min, Offset.unsafe(1)),
              segmentSize,
              SeqNr.min,
              correlationId = correlationId.some,
            ),
            origin.some,
          ),
          Action.InsertRecords(key, SegmentNr.min, 1),
        ),
        metaJournal = Map(
          (
            (topic0, segment),
            Map(
              (
                id,
                MetaJournalEntry(
                  journalHead = JournalHead(
                    partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(3)),
                    segmentSize     = segmentSize,
                    seqNr           = SeqNr.min,
                    deleteTo        = SeqNr.min.toDeleteTo.some,
                    expiry          = none,
                    correlationId   = correlationId.some,
                  ),
                  created = timestamp0,
                  updated = timestamp1,
                  origin  = origin.some,
                ),
              ),
            ),
          ),
        ),
      )
      val result = stateT.run(State.empty)
      result shouldEqual (expected, ()).pure[Try]
    }

    test(s"not repeat deletions, $suffix") {
      val id      = "id"
      val key     = Key(id = id, topic = topic0)
      val segment = segmentOfId(key)
      val stateT = journal.delete(
        key = key,
        Partition.min,
        Offset.unsafe(1),
        timestamp = timestamp1,
        deleteTo  = SeqNr.min.toDeleteTo,
        origin    = origin.some,
      )

      val initial = State
        .empty
        .copy(
          metaJournal = Map(
            (
              (topic0, segment),
              Map(
                (
                  id,
                  MetaJournalEntry(
                    journalHead = JournalHead(
                      partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(2)),
                      segmentSize     = segmentSize,
                      seqNr           = SeqNr.min,
                      deleteTo        = SeqNr.min.toDeleteTo.some,
                      expiry          = none,
                      correlationId   = correlationId.some,
                    ),
                    created = timestamp0,
                    updated = timestamp0,
                    origin  = origin.some,
                  ),
                ),
              ),
            ),
          ),
          journal = Map(((key, SegmentNr.min), Map(((SeqNr.min, timestamp0), record)))),
        )

      val expected = State(
        metaJournal = Map(
          (
            (topic0, segment),
            Map(
              (
                id,
                MetaJournalEntry(
                  journalHead = JournalHead(
                    partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(2)),
                    segmentSize     = segmentSize,
                    seqNr           = SeqNr.min,
                    deleteTo        = SeqNr.min.toDeleteTo.some,
                    expiry          = none,
                    correlationId   = correlationId.some,
                  ),
                  created = timestamp0,
                  updated = timestamp0,
                  origin  = origin.some,
                ),
              ),
            ),
          ),
        ),
        journal = Map(((key, SegmentNr.min), Map(((SeqNr.min, timestamp0), record)))),
      )

      val actual = stateT.run(initial)
      actual shouldEqual (expected, false).pure[Try]
    }

    test(s"purge, $suffix") {
      val id      = "id"
      val key     = Key(id, topic0)
      val segment = segmentOfId(key)
      val stateT = for {
        _ <- journal.append(
          key = key,
          Partition.min,
          Offset.unsafe(3),
          timestamp   = timestamp0,
          expireAfter = none,
          events = Nel.of(
            eventRecordOf(seqNr = SeqNr.unsafe(1), partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(1))),
            eventRecordOf(seqNr = SeqNr.unsafe(2), partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(2))),
          ),
        )
        _ <- journal.purge(key, Partition.min, Offset.unsafe(4), timestamp1)
      } yield {}

      val actual = stateT.run(State.empty)
      val expected = State(
        actions = List(
          Action.Delete(key, segment),
          Action.UpdateDeleteTo(
            key,
            segment,
            PartitionOffset(Partition.min, Offset.unsafe(3)),
            timestamp1,
            SeqNr.unsafe(2).toDeleteTo,
          ),
          Action.InsertMetaJournal(
            key,
            segment,
            created = timestamp0,
            updated = timestamp0,
            JournalHead(
              PartitionOffset(Partition.min, Offset.unsafe(3)),
              segmentSize,
              SeqNr.unsafe(2),
              correlationId = correlationId.some,
            ),
            origin.some,
          ),
          Action.InsertRecords(key, SegmentNr.min, 2),
        ),
      )
      actual shouldEqual (expected, ()).pure[Try]
    }

    test(s"repeat purge again for the same offset, $suffix") {
      val id      = "id"
      val key     = Key(id = id, topic = topic0)
      val segment = segmentOfId(key)
      val stateT = for {
        _ <- journal.append(key, partitionOffset.partition, partitionOffset.offset, timestamp0, none, Nel.of(record))
        _ <- journal.purge(key, partitionOffset.partition, partitionOffset.offset, timestamp0)
      } yield {}

      val expected = State(
        actions = List(
          Action.Delete(key, segment),
          Action.UpdateDeleteTo(key, segment, partitionOffset, timestamp0, SeqNr.min.toDeleteTo),
          Action.InsertMetaJournal(
            key,
            segment,
            created = timestamp0,
            updated = timestamp0,
            JournalHead(partitionOffset, segmentSize, SeqNr.min, correlationId = correlationId.some),
            origin.some,
          ),
          Action.InsertRecords(key, SegmentNr.min, 1),
        ),
      )
      val result = stateT.run(State.empty)
      result shouldEqual (expected, ()).pure[Try]
    }

    test(s"ignore purge, $suffix") {
      val id     = "id"
      val key    = Key(id, topic0)
      val stateT = journal.purge(key, Partition.min, Offset.unsafe(4), timestamp1)
      val actual = stateT.run(State.empty)
      actual shouldEqual (State.empty, false).pure[Try]
    }

    test(s"purge meta journal only, $suffix") {
      val id      = "id"
      val key     = Key(id, topic0)
      val segment = segmentOfId(key)
      val stateT = for {
        _ <- journal.append(
          key = key,
          Partition.min,
          Offset.unsafe(3),
          timestamp   = timestamp0,
          expireAfter = none,
          events = Nel.of(
            eventRecordOf(seqNr = SeqNr.unsafe(1), partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(1))),
            eventRecordOf(seqNr = SeqNr.unsafe(2), partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(2))),
          ),
        )
        _ <- journal.delete(key, Partition.min, Offset.unsafe(4), timestamp0, SeqNr.unsafe(2).toDeleteTo, origin.some)
        _ <- journal.purge(key, Partition.min, Offset.unsafe(5), timestamp1)
      } yield {}

      val actual = stateT.run(State.empty)
      val expected = State(
        actions = List(
          Action.Delete(key, segment),
          Action.UpdateDeleteTo(
            key,
            segment,
            PartitionOffset(Partition.min, Offset.unsafe(4)),
            timestamp0,
            SeqNr.unsafe(2).toDeleteTo,
          ),
          Action.InsertMetaJournal(
            key,
            segment,
            created = timestamp0,
            updated = timestamp0,
            JournalHead(
              PartitionOffset(Partition.min, Offset.unsafe(3)),
              segmentSize,
              SeqNr.unsafe(2),
              correlationId = correlationId.some,
            ),
            origin.some,
          ),
          Action.InsertRecords(key, SegmentNr.min, 2),
        ),
      )
      actual shouldEqual (expected, ()).pure[Try]
    }

    test(s"not set correlation ID, $suffix") {
      val id      = "id"
      val key     = Key(id = id, topic = topic0)
      val segment = segmentOfId(key)

      def partitionOffset(offset: Long) = PartitionOffset(Partition.min, Offset.unsafe(offset))

      val record0 = eventRecordOf(SeqNr.min, partitionOffset(0)).withoutCorrelationId
      val record1 = eventRecordOf(SeqNr.min.next[Id], partitionOffset(1)).withoutCorrelationId

      val stateT = journal.append(
        key = key,
        Partition.min,
        Offset.unsafe(1),
        timestamp   = timestamp1,
        expireAfter = none,
        events      = Nel.of(record1),
      )

      val initial = State
        .empty
        .copy(
          metaJournal = Map(
            (
              (topic0, segment),
              Map(
                (
                  id,
                  MetaJournalEntry(
                    journalHead = JournalHead(
                      partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(0)),
                      segmentSize     = segmentSize,
                      seqNr           = SeqNr.min,
                      correlationId   = none,
                    ),
                    created = timestamp0,
                    updated = timestamp0,
                    origin  = origin.some,
                  ),
                ),
              ),
            ),
          ),
          journal = Map(
            (
              (key, SegmentNr.min),
              Map(
                ((SeqNr.min, timestamp0), record0),
              ),
            ),
          ),
        )

      val expected = State(
        actions = List(
          Action.UpdateSeqNr(key, segment, PartitionOffset(Partition.min, Offset.unsafe(1)), timestamp1, SeqNr.min.next[Id]),
          Action.InsertRecords(key, SegmentNr.min, 1),
        ),
        metaJournal = Map(
          (
            (topic0, segment),
            Map(
              (
                id,
                MetaJournalEntry(
                  journalHead = JournalHead(
                    partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(1)),
                    segmentSize     = segmentSize,
                    seqNr           = SeqNr.min.next[Id],
                    expiry          = none,
                    correlationId   = none,
                  ),
                  created = timestamp0,
                  updated = timestamp1,
                  origin  = origin.some,
                ),
              ),
            ),
          ),
        ),
        journal = Map(
          (
            (key, SegmentNr.min),
            Map(
              ((SeqNr.min, timestamp0), record0),
              ((SeqNr.min.next[Id], timestamp0), record1),
            ),
          ),
        ),
      )

      val actual = stateT.run(initial)
      actual shouldEqual (expected, true).pure[Try]
    }

    test(s"not update correlation ID, $suffix") {
      val id      = "id"
      val key     = Key(id = id, topic = topic0)
      val segment = segmentOfId(key)
      val cid0    = CorrelationId.unsafe("some-correlation-id")

      def partitionOffset(offset: Long) = PartitionOffset(Partition.min, Offset.unsafe(offset))

      val record0 = eventRecordOf(SeqNr.min, partitionOffset(0)).withCorrelationId(cid0)
      val record1 = eventRecordOf(SeqNr.min.next[Id], partitionOffset(1)).withoutCorrelationId

      val stateT = journal.append(
        key = key,
        Partition.min,
        Offset.unsafe(1),
        timestamp   = timestamp1,
        expireAfter = none,
        events      = Nel.of(record1),
      )

      val initial = State
        .empty
        .copy(
          metaJournal = Map(
            (
              (topic0, segment),
              Map(
                (
                  id,
                  MetaJournalEntry(
                    journalHead = JournalHead(
                      partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(0)),
                      segmentSize     = segmentSize,
                      seqNr           = SeqNr.min,
                      correlationId   = cid0.some,
                    ),
                    created = timestamp0,
                    updated = timestamp0,
                    origin  = origin.some,
                  ),
                ),
              ),
            ),
          ),
          journal = Map(
            (
              (key, SegmentNr.min),
              Map(
                ((SeqNr.min, timestamp0), record0),
              ),
            ),
          ),
        )

      val expected = State(
        actions = List(
          Action.UpdateSeqNr(key, segment, PartitionOffset(Partition.min, Offset.unsafe(1)), timestamp1, SeqNr.min.next[Id]),
          Action.InsertRecords(key, SegmentNr.min, 1),
        ),
        metaJournal = Map(
          (
            (topic0, segment),
            Map(
              (
                id,
                MetaJournalEntry(
                  journalHead = JournalHead(
                    partitionOffset = PartitionOffset(Partition.min, Offset.unsafe(1)),
                    segmentSize     = segmentSize,
                    seqNr           = SeqNr.min.next[Id],
                    expiry          = none,
                    correlationId   = cid0.some,
                  ),
                  created = timestamp0,
                  updated = timestamp1,
                  origin  = origin.some,
                ),
              ),
            ),
          ),
        ),
        journal = Map(
          (
            (key, SegmentNr.min),
            Map(
              ((SeqNr.min, timestamp0), record0),
              ((SeqNr.min.next[Id], timestamp0), record1.withCorrelationId(cid0)),
            ),
          ),
        ),
      )

      val actual = stateT.run(initial)
      actual shouldEqual (expected, true).pure[Try]
    }
  }
}

object ReplicatedCassandraTest {

  val insertRecords: JournalStatements.InsertRecords[StateT] = { (key, segment, events) =>
    {
      StateT.unit { state =>
        val k = (key, segment)
        val entries = state
          .journal
          .getOrElse(k, Map.empty)

        val entries1 = events.foldLeft(entries) { (entries, event) =>
          entries.updated((event.seqNr, event.timestamp), event)
        }

        val journal1 = state.journal.updated(k, entries1)
        state
          .copy(journal = journal1)
          .append(Action.InsertRecords(key, segment, events.size))
      }
    }
  }

  val deleteRecordsTo: JournalStatements.DeleteTo[StateT] = { (key, segment, seqNr) =>
    {
      StateT.unit { state =>
        val k       = (key, segment)
        val journal = state.journal
        val entries = journal
          .getOrElse(k, Map.empty)
          .filter { case ((a, _), _) => a > seqNr }
        val journal1 = if (entries.isEmpty) journal - k else journal.updated(k, entries)
        state.copy(journal = journal1)
      }
    }
  }

  val deleteRecords: JournalStatements.Delete[StateT] = { (key, segment) =>
    {
      StateT.unit { state =>
        val k = (key, segment)
        state.copy(journal = state.journal - k)
      }
    }
  }

  val insertMetaJournal: MetaJournalStatements.Insert[StateT] = { (key, segment, created, updated, journalHead, origin) =>
    {
      StateT.unit { state =>
        val entry = MetaJournalEntry(journalHead = journalHead, created = created, updated = updated, origin = origin)
        val entries = state
          .metaJournal
          .getOrElse((key.topic, segment), Map.empty)
          .updated(key.id, entry)
        state
          .copy(metaJournal = state.metaJournal.updated((key.topic, segment), entries))
          .append(Action.InsertMetaJournal(key, segment, created, updated, journalHead, origin))
      }
    }
  }

  val selectMetaJournal: MetaJournalStatements.SelectJournalHead[StateT] = { (key, segment) =>
    {
      StateT.success { state =>
        val journalHead = for {
          entries <- state.metaJournal.get((key.topic, segment))
          entry   <- entries.get(key.id)
        } yield {
          entry.journalHead
        }
        (state, journalHead)
      }
    }
  }

  val updateMetaJournal: MetaJournalStatements.Update[StateT] = { (key, segment, partitionOffset, timestamp, seqNr, deleteTo) =>
    {
      StateT.unit { state =>
        state.updateMetaJournal(key, segment) { entry =>
          entry.copy(
            journalHead = entry.journalHead.copy(partitionOffset = partitionOffset, seqNr = seqNr, deleteTo = deleteTo.some),
            updated     = timestamp,
          )
        }
      }
    }
  }

  val updateSeqNrMetaJournal: MetaJournalStatements.UpdateSeqNr[StateT] = { (key, segment, partitionOffset, timestamp, seqNr) =>
    {
      StateT.unit { state =>
        state
          .updateMetaJournal(key, segment) { entry =>
            entry.copy(
              journalHead = entry.journalHead.copy(partitionOffset = partitionOffset, seqNr = seqNr),
              updated     = timestamp,
            )
          }
          .append(Action.UpdateSeqNr(key, segment, partitionOffset, timestamp, seqNr))
      }
    }
  }

  val updateExpiryMetaJournal: MetaJournalStatements.UpdateExpiry[StateT] = {
    (key, segment, partitionOffset, timestamp, seqNr, expiry) =>
      {
        StateT.unit { state =>
          state
            .updateMetaJournal(key, segment) { entry =>
              entry.copy(
                journalHead = entry.journalHead.copy(partitionOffset = partitionOffset, seqNr = seqNr, expiry = expiry.some),
                updated     = timestamp,
              )
            }
            .append(Action.UpdateExpiry(key, segment, partitionOffset, timestamp, seqNr, expiry))
        }
      }
  }

  val updateDeleteToMetaJournal: MetaJournalStatements.UpdateDeleteTo[StateT] = {
    (key, segment, partitionOffset, timestamp, deleteTo) =>
      {
        StateT.unit { state =>
          state
            .updateMetaJournal(key, segment) { entry =>
              entry.copy(
                journalHead = entry.journalHead.copy(partitionOffset = partitionOffset, deleteTo = deleteTo.some),
                updated     = timestamp,
              )
            }
            .append(Action.UpdateDeleteTo(key, segment, partitionOffset, timestamp, deleteTo))
        }
      }
  }

  val updatePartitionOffsetMetaJournal: MetaJournalStatements.UpdatePartitionOffset[StateT] = {
    (key, segment, partitionOffset, timestamp) =>
      {
        StateT.unit { state =>
          state
            .updateMetaJournal(key, segment) { entry =>
              entry.copy(journalHead = entry.journalHead.copy(partitionOffset = partitionOffset), updated = timestamp)
            }
            .append(Action.UpdatePartitionOffset(key, segment, partitionOffset, timestamp))
        }
      }
  }

  val deleteMetaJournal: MetaJournalStatements.Delete[StateT] = { (key, segment) =>
    {
      StateT.unit { state =>
        val k = (key.topic, segment)
        val state1 = for {
          entries <- state.metaJournal.get(k)
          _       <- entries.get(key.id)
        } yield {
          val entries1 = entries - key.id
          val metaJournal = if (entries1.isEmpty) {
            state.metaJournal - k
          } else {
            state.metaJournal.updated(k, entries1)
          }
          state.copy(metaJournal = metaJournal)
        }
        state1
          .getOrElse(state)
          .append(Action.Delete(key, segment))
      }
    }
  }

  val deleteExpiryMetaJournal: MetaJournalStatements.DeleteExpiry[StateT] = { (key, segment) =>
    {
      StateT.unit { state =>
        state
          .updateMetaJournal(key, segment) { entry =>
            entry.copy(journalHead = entry.journalHead.copy(expiry = none))
          }
          .append(Action.DeleteExpiry(key, segment))
      }
    }
  }

  val selectOffset: PointerStatements.SelectOffset[StateT] = { (topic, partition) =>
    {
      StateT.success { state =>
        val offset = for {
          pointers <- state.pointers.get(topic)
          pointer  <- pointers.get(partition)
        } yield {
          pointer.offset
        }
        (state, offset)
      }
    }
  }

  val selectOffset2: Pointer2Statements.SelectOffset[StateT] = { (_, _) =>
    none[Offset].pure[StateT]
  }

  val selectPointer: PointerStatements.Select[StateT] = { (_, _) =>
    PointerStatements.Select.Result(Instant.EPOCH.some).some.pure[StateT]
  }

  val selectPointer2: Pointer2Statements.Select[StateT] = { (_, _) =>
    none[Pointer2Statements.Select.Result].pure[StateT]
  }

  val insertPointer: PointerStatements.Insert[StateT] = { (topic, partition, offset, created, updated) =>
    {
      StateT.unit { state =>
        val entry = PointerEntry(offset = offset, created = created, updated = updated)
        val entries = state
          .pointers
          .getOrElse(topic, Map.empty)
          .updated(partition, entry)
        state.copy(pointers = state.pointers.updated(topic, entries))
      }
    }
  }

  val insertPointer2: Pointer2Statements.Insert[StateT] = { (_, _, _, _, _) =>
    ().pure[StateT]
  }

  val updatePointer: PointerStatements.Update[StateT] = { (topic, partition, offset, timestamp) =>
    {
      StateT.unit { state =>
        state.updatePointer(topic, partition) { entry =>
          entry.copy(offset = offset, updated = timestamp)
        }
      }
    }
  }

  val updatePointer2: Pointer2Statements.Update[StateT] = { (_, _, _, _) =>
    ().pure[StateT]
  }

  val updatePointerCreated2: Pointer2Statements.UpdateCreated[StateT] = { (_, _, _, _, _) =>
    ().pure[StateT]
  }

  val selectTopics: PointerStatements.SelectTopics[StateT] = { () =>
    {
      StateT.success { state =>
        val topics = state.pointers.keySet.toSortedSet
        (state, topics)
      }
    }
  }

  val selectTopics2: Pointer2Statements.SelectTopics[StateT] = { () =>
    SortedSet.empty[Topic].pure[StateT]
  }

  val statements: ReplicatedCassandra.Statements[StateT] = {

    val metaJournal = ReplicatedCassandra.MetaJournalStatements(
      selectMetaJournal,
      insertMetaJournal,
      updateMetaJournal,
      updateSeqNrMetaJournal,
      updateExpiryMetaJournal,
      updateDeleteToMetaJournal,
      updatePartitionOffsetMetaJournal,
      deleteMetaJournal,
      deleteExpiryMetaJournal,
    )

    ReplicatedCassandra.Statements(
      insertRecords,
      deleteRecordsTo,
      deleteRecords,
      metaJournal,
      selectOffset,
      selectOffset2,
      selectPointer,
      selectPointer2,
      insertPointer,
      insertPointer2,
      updatePointer,
      updatePointer2,
      updatePointerCreated2,
      selectTopics,
      selectTopics2,
    )
  }

  implicit val failStatT: Fail[StateT] = new Fail[StateT] {
    def fail[A](a: String): StateT[A] = {
      StateT { _ => JournalError(a).raiseError[Try, (State, A)] }
    }
  }

  implicit val syncStateT: Sync[StateT] = new Sync[StateT] with MonadCancelFromMonadError[StateT, Throwable] {

    val F = IndexedStateT.catsDataMonadErrorForIndexedStateT(catsStdInstancesForTry)

    override def rootCancelScope: CancelScope = CancelScope.Uncancelable

    override def forceR[A, B](fa: StateT[A])(fb: StateT[B]): StateT[B] = fa.redeemWith(_ => fb, _ => fb)

    override def uncancelable[A](body: Poll[StateT] => StateT[A]): StateT[A] = body(new Poll[StateT] {
      override def apply[X](fa: StateT[X]): StateT[X] = fa
    })

    override def canceled: StateT[Unit] = ().pure[StateT]

    override def onCancel[A](fa: StateT[A], fin: StateT[Unit]): StateT[A] = fa

    override def suspend[A](hint: Sync.Type)(thunk: => A): StateT[A] = cats.data.StateT.pure(thunk)

    override def monotonic: StateT[FiniteDuration] =
      cats.data.StateT.pure(FiniteDuration(System.nanoTime(), TimeUnit.NANOSECONDS))

    override def realTime: StateT[FiniteDuration] =
      cats.data.StateT.pure(FiniteDuration(System.currentTimeMillis(), TimeUnit.MILLISECONDS))
  }

  implicit val parallel: Parallel[StateT] = Parallel.identity[StateT]

  final case class PointerEntry(offset: Offset, created: Instant, updated: Instant)

  sealed trait Action

  object Action {

    final case class InsertRecords(key: Key, segment: SegmentNr, records: Int) extends Action

    final case class InsertMetaJournal(
      key: Key,
      segment: SegmentNr,
      created: Instant,
      updated: Instant,
      journalHead: JournalHead,
      origin: Option[Origin],
    ) extends Action

    final case class UpdateSeqNr(
      key: Key,
      segment: SegmentNr,
      partitionOffset: PartitionOffset,
      timestamp: Instant,
      seqNr: SeqNr,
    ) extends Action

    final case class UpdateExpiry(
      key: Key,
      segment: SegmentNr,
      partitionOffset: PartitionOffset,
      timestamp: Instant,
      seqNr: SeqNr,
      expiry: Expiry,
    ) extends Action

    final case class UpdateDeleteTo(
      key: Key,
      segment: SegmentNr,
      partitionOffset: PartitionOffset,
      timestamp: Instant,
      deleteTo: DeleteTo,
    ) extends Action

    final case class UpdatePartitionOffset(
      key: Key,
      segment: SegmentNr,
      partitionOffset: PartitionOffset,
      timestamp: Instant,
    ) extends Action

    final case class Delete(key: Key, segment: SegmentNr) extends Action

    final case class DeleteExpiry(key: Key, segment: SegmentNr) extends Action
  }

  final case class State(
    actions: List[Action]                                                                      = List.empty,
    pointers: Map[Topic, Map[Partition, PointerEntry]]                                         = Map.empty,
    metaJournal: Map[(Topic, SegmentNr), Map[String, MetaJournalEntry]]                        = Map.empty,
    journal: Map[(Key, SegmentNr), Map[(SeqNr, Instant), EventRecord[EventualPayloadAndType]]] = Map.empty,
  )

  object State {

    val empty: State = State()

    implicit class StateOps(val self: State) extends AnyVal {

      def append(action: Action): State = self.copy(actions = action :: self.actions)

      def updateMetaJournal(key: Key, segment: SegmentNr)(f: MetaJournalEntry => MetaJournalEntry): State = {
        val state = for {
          entries <- self.metaJournal.get((key.topic, segment))
          entry   <- entries.get(key.id)
        } yield {
          val entry1   = f(entry)
          val entries1 = entries.updated(key.id, entry1)
          self.copy(metaJournal = self.metaJournal.updated((key.topic, segment), entries1))
        }
        state getOrElse self
      }

      def updatePointer(topic: Topic, partition: Partition)(f: PointerEntry => PointerEntry): State = {
        val state = for {
          entries <- self.pointers.get(topic)
          entry   <- entries.get(partition)
        } yield {
          val entry1   = f(entry)
          val entries1 = entries.updated(partition, entry1)
          self.copy(pointers = self.pointers.updated(topic, entries1))
        }
        state getOrElse self
      }

    }
  }

  type StateT[A] = cats.data.StateT[Try, State, A]

  object StateT {

    def apply[A](f: State => Try[(State, A)]): StateT[A] = cats.data.StateT[Try, State, A](f)

    def success[A](f: State => (State, A)): StateT[A] = apply { s => f(s).pure[Try] }

    def unit(f: State => State): StateT[Unit] = success[Unit] { a => (f(a), ()) }
  }
}
