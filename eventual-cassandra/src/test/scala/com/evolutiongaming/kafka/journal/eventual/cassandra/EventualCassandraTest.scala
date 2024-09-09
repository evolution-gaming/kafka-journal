package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Id
import cats.data.NonEmptyList as Nel
import cats.effect.Concurrent
import cats.syntax.all.*
import com.evolutiongaming.catshelper.DataHelper.*
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.journal.*
import com.evolutiongaming.kafka.journal.Journal.DataIntegrityConfig
import com.evolutiongaming.kafka.journal.eventual.EventualPayloadAndType
import com.evolutiongaming.kafka.journal.util.ConcurrentOf
import com.evolutiongaming.kafka.journal.util.SkafkaHelper.*
import com.evolutiongaming.kafka.journal.util.StreamHelper.*
import com.evolutiongaming.kafka.journal.util.TemporalHelper.*
import com.evolutiongaming.skafka.{Offset, Partition, Topic}
import com.evolutiongaming.sstream.FoldWhile.*
import com.evolutiongaming.sstream.Stream
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import play.api.libs.json.Json

import java.time.Instant
import scala.concurrent.duration.*
import scala.util.{Failure, Try}

class EventualCassandraTest extends AnyFunSuite with Matchers {
  import EventualCassandraTest.*

  private val timestamp0      = Instant.now()
  private val timestamp1      = timestamp0 + 1.minute
  private val topic0          = "topic0"
  private val topic1          = "topic1"
  private val partitionOffset = PartitionOffset.empty
  private val origin          = Origin("origin")
  private val version         = Version.current
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
    )
  }

  for {
    segmentSize <- List(SegmentSize.min, SegmentSize.default, SegmentSize.max)
    segments    <- List(Segments.min, Segments.old)
  } {
    implicit val log: Log[StateT] = Log.empty[StateT]

    val config = new DataIntegrityConfig {
      override def seqNrUniqueness         = true
      override def correlateEventsWithMeta = true
    }
    val segmentOf    = SegmentOf[Id](segments)
    val segmentNrsOf = SegmentNrsOf[StateT](first = segments, Segments.default)
    val statements   = statementsOf(segmentNrsOf, Segments.default)
    val journal      = EventualCassandra.apply2(statements, config)

    val suffix = s"segmentSize: $segmentSize, segments: $segments"

    test(s"pointers, $suffix") {
      val stateT = for {
        offset <- journal.offset(topic0, Partition.min)
        _       = offset shouldEqual none
        _      <- insertPointer(topic0, Partition.min, Offset.min, created = timestamp0, updated = timestamp0)
        offset <- journal.offset(topic0, Partition.min)
        _       = offset shouldEqual Offset.min.some
        _      <- updatePointer(topic0, Partition.min, Offset.unsafe(1), timestamp = timestamp1)
        offset <- journal.offset(topic0, Partition.min)
        _       = offset shouldEqual Offset.unsafe(1).some
        _      <- insertPointer(topic1, Partition.min, Offset.min, created = timestamp0, updated = timestamp0)
        offset <- journal.offset(topic0, Partition.min)
        _       = offset shouldEqual Offset.unsafe(1).some
      } yield {}

      val expected = State(
        pointers = Map(
          (topic0, Map((Partition.min, PointerEntry(Offset.unsafe(1), created = timestamp0, updated = timestamp1)))),
          (topic1, Map((Partition.min, PointerEntry(Offset.min, created = timestamp0, updated = timestamp0)))),
        ),
      )
      stateT.run(State.empty) shouldEqual (expected, ()).pure[Try]
    }

    test(s"pointer, $suffix") {
      val id      = "id"
      val key     = Key(id = id, topic = topic0)
      val segment = segmentOf(key)
      val seqNr   = SeqNr.min
      val stateT = for {
        pointer    <- journal.pointer(key)
        _           = pointer shouldEqual none
        journalHead = JournalHead(partitionOffset, segmentSize, seqNr)
        _          <- insertMetaJournal(key, segment, timestamp0, timestamp0, journalHead, origin.some)
        pointer    <- journal.pointer(key)
        _           = pointer shouldEqual JournalPointer(partitionOffset, seqNr).some
        _          <- updateMetaJournal(key, segment, partitionOffset, timestamp1, SeqNr.max, seqNr.toDeleteTo)
        pointer    <- journal.pointer(key)
        _           = pointer shouldEqual JournalPointer(partitionOffset, SeqNr.max).some
      } yield {}
      val expected = State(
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
                    seqNr           = SeqNr.max,
                    deleteTo        = SeqNr.min.toDeleteTo.some,
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
      stateT.run(State.empty) shouldEqual (expected, ()).pure[Try]
    }

    for {
      seqNr <- List(SeqNr.min, SeqNr.unsafe(5), SeqNr.max)
    } {
      test(s"read, seqNr: $seqNr, $suffix") {

        val id      = "id"
        val key     = Key(id = id, topic = topic0)
        val segment = segmentOf(key)
        val record1 = {
          record.copy(
            timestamp       = timestamp1,
            event           = record.event.copy(seqNr = record.seqNr.next[Id]),
            partitionOffset = record.partitionOffset.copy(offset = record.partitionOffset.offset.inc[Try].get),
          )
        }

        val stateT = for {
          records <- journal.read(key, seqNr).toList
          _        = records shouldEqual List.empty
          head     = JournalHead(partitionOffset, segmentSize, record.seqNr)
          _       <- insertMetaJournal(key, segment, timestamp0, timestamp0, head, origin.some)
          _       <- insertRecords(key, SegmentNr.min, Nel.of(record))
          records <- journal.read(key, seqNr).toList
          _        = records shouldEqual List(record).filter(_.seqNr >= seqNr)
          _       <- updateMetaJournalSeqNr(key, segment, partitionOffset, timestamp1, record1.seqNr)
          _       <- insertRecords(key, SegmentNr.min, Nel.of(record1))
          records <- journal.read(key, seqNr).toList
          _        = records shouldEqual List(record, record1).filter(_.seqNr >= seqNr)
          _       <- updateMetaJournal(key, segment, partitionOffset, timestamp1, record1.seqNr, record.seqNr.toDeleteTo)
          records <- journal.read(key, seqNr).toList
          _        = records shouldEqual List(record1).filter(_.seqNr >= seqNr)
        } yield {}

        val expected = State(
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
                      seqNr           = record1.seqNr,
                      deleteTo        = record.seqNr.toDeleteTo.some,
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
            ((key, SegmentNr.min), Map(((record.seqNr, record.timestamp), record), ((record1.seqNr, record1.timestamp), record1))),
          ),
        )
        stateT.run(State.empty) shouldEqual (expected, ()).pure[Try]
      }
    }

    test(s"read duplicated seqNr, $suffix") {
      val seqNr   = SeqNr.min
      val key     = Key(id = "id", topic = topic0)
      val segment = segmentOf(key)
      val record1 = record.copy(timestamp = timestamp1)

      val stateT = for {
        records <- journal.read(key, seqNr).toList
        _        = records shouldEqual List.empty
        head     = JournalHead(partitionOffset, segmentSize, record.seqNr)
        _       <- insertMetaJournal(key, segment, timestamp0, timestamp0, head, origin.some)
        _       <- insertRecords(key, SegmentNr.min, Nel.of(record, record1))
        _       <- journal.read(key, seqNr).toList
      } yield {}

      stateT.run(State.empty) should matchPattern { case Failure(_: JournalError) => }
    }

    test(s"ids, $suffix") {
      val id0 = "id0"
      val id1 = "id1"

      def idsOf = journal
        .ids(topic0)
        .toList
        .map { _.sorted }

      val stateT = for {
        ids <- idsOf
        _    = ids shouldEqual List.empty
        head = JournalHead(partitionOffset, segmentSize, SeqNr.min)
        _   <- insertMetaJournal(Key(id = id0, topic = topic0), SegmentNr.min, timestamp0, timestamp0, head, origin.some)
        ids <- idsOf
        _    = ids shouldEqual List(id0)
        _   <- insertMetaJournal(Key(id = id1, topic = topic0), SegmentNr.min, timestamp0, timestamp0, head, origin.some)
        ids <- idsOf
        _    = ids shouldEqual List(id0, id1)
      } yield {}

      stateT
        .run(State.empty)
        .map { case (_, a) => a } shouldEqual ().pure[Try]
    }

    test(s"read only events that corelate with meta, $suffix") {
      val seqNr   = SeqNr.min
      val key     = Key(id = "id", topic = topic0)
      val segment = segmentOf(key)
      val actual  = CorrelationId.unsafe("actual")
      val legacy  = CorrelationId.unsafe("legacy")

      val stateT = journal.read(key, seqNr).toList

      val record1 = eventRecordOf(seqNr, partitionOffset).withCorrelationId(legacy)
      val record2 = eventRecordOf(seqNr.next[Id], partitionOffset).withCorrelationId(actual)
      val initial = State(
        metaJournal = Map(
          (topic0, segment) -> Map(
            key.id -> MetaJournalEntry(
              journalHead = JournalHead(partitionOffset, segmentSize, seqNr.next[Id], correlationId = actual.some),
              created     = timestamp1,
              updated     = timestamp1,
              origin      = origin.some,
            ),
          ),
        ),
        journal = Map(
          (key, SegmentNr.min) -> Map(
            (seqNr, timestamp0)          -> record1,
            (seqNr.next[Id], timestamp1) -> record2,
          ),
        ),
      )

      stateT.run(initial).map { case (_, events) => events } shouldEqual List(record2).pure[Try]
    }

    test(s"read events with duplicated seqNr if only one 'branch' correlate with meta, $suffix") {
      val seqNr         = SeqNr.min
      val key           = Key(id = "id", topic = topic0)
      val segment       = segmentOf(key)
      val correlationId = CorrelationId.unsafe("actual")

      val stateT = journal.read(key, seqNr).toList

      val record1 = eventRecordOf(seqNr, partitionOffset).withoutCorrelationId
      val record2 = eventRecordOf(seqNr, partitionOffset).withCorrelationId(correlationId)
      val initial = State(
        metaJournal = Map(
          (topic0, segment) -> Map(
            key.id -> MetaJournalEntry(
              journalHead = JournalHead(partitionOffset, segmentSize, seqNr, correlationId = correlationId.some),
              created     = timestamp1,
              updated     = timestamp1,
              origin      = origin.some,
            ),
          ),
        ),
        journal = Map(
          (key, SegmentNr.min) -> Map(
            (seqNr, timestamp0) -> record1,
            (seqNr, timestamp1) -> record2,
          ),
        ),
      )

      stateT.run(initial).map { case (_, events) => events } shouldEqual List(record2).pure[Try]
    }
  }
}

object EventualCassandraTest {

  val insertRecords: JournalStatements.InsertRecords[StateT] = {
    (key: Key, segment: SegmentNr, events: Nel[EventRecord[EventualPayloadAndType]]) =>
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
          state.copy(journal = journal1)
        }
      }
  }

  val deleteRecords: JournalStatements.DeleteTo[StateT] = { (key: Key, segment: SegmentNr, seqNr: SeqNr) =>
    {
      StateT.unit { state =>
        val k       = (key, segment)
        val journal = state.journal
        val entries = journal
          .getOrElse(k, Map.empty)
          .filter { case ((a, _), _) => a <= seqNr }
        val journal1 = if (entries.isEmpty) journal - k else journal.updated(k, entries)
        state.copy(journal = journal1)
      }
    }
  }

  val insertMetaJournal: MetaJournalStatements.Insert[StateT] = {
    (key: Key, segment: SegmentNr, created: Instant, updated: Instant, head: JournalHead, origin: Option[Origin]) =>
      {
        StateT.unit { state =>
          val entry = MetaJournalEntry(
            journalHead = JournalHead(
              partitionOffset = head.partitionOffset,
              segmentSize     = head.segmentSize,
              seqNr           = head.seqNr,
              deleteTo        = head.deleteTo,
            ),
            created = created,
            updated = updated,
            origin  = origin,
          )
          val entries = state
            .metaJournal
            .getOrElse((key.topic, segment), Map.empty)
            .updated(key.id, entry)
          state.copy(metaJournal = state.metaJournal.updated((key.topic, segment), entries))
        }
      }
  }

  val selectJournalHead0: MetaJournalStatements.SelectJournalHead[StateT] = { (key: Key, segment: SegmentNr) =>
    {
      StateT.success { state =>
        val head = for {
          entries <- state.metaJournal.get((key.topic, segment))
          entry   <- entries.get(key.id)
        } yield {
          entry.journalHead
        }
        (state, head)
      }
    }
  }

  val selectJournalPointer0: MetaJournalStatements.SelectJournalPointer[StateT] = { (key: Key, segment: SegmentNr) =>
    {
      StateT.success { state =>
        val pointer = for {
          entries <- state.metaJournal.get((key.topic, segment))
          entry   <- entries.get(key.id)
        } yield {
          JournalPointer(partitionOffset = entry.journalHead.partitionOffset, seqNr = entry.journalHead.seqNr)
        }
        (state, pointer)
      }
    }
  }

  val selectIds0: MetaJournalStatements.SelectIds[StateT] = { (topic, segmentNr) =>
    {
      Stream.lift {
        StateT.success { state =>
          val ids = state
            .metaJournal
            .get((topic, segmentNr))
            .toList
            .flatMap { _.keys.toList }
          (state, Stream[StateT].apply(ids))
        }
      }.flatten
    }
  }

  val updateMetaJournal: MetaJournalStatements.Update[StateT] = {
    (key: Key, segment: SegmentNr, partitionOffset: PartitionOffset, timestamp: Instant, seqNr: SeqNr, deleteTo: DeleteTo) =>
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

  val updateMetaJournalSeqNr: MetaJournalStatements.UpdateSeqNr[StateT] = {
    (key: Key, segment: SegmentNr, partitionOffset: PartitionOffset, timestamp: Instant, seqNr: SeqNr) =>
      {
        StateT.unit { state =>
          state.updateMetaJournal(key, segment) { entry =>
            entry.copy(
              journalHead = entry.journalHead.copy(partitionOffset = partitionOffset, seqNr = seqNr),
              updated     = timestamp,
            )
          }
        }
      }
  }

  val updateMetaJournalDeleteTo: MetaJournalStatements.UpdateDeleteTo[StateT] = {
    (key: Key, segment: SegmentNr, partitionOffset: PartitionOffset, timestamp: Instant, deleteTo: DeleteTo) =>
      {
        StateT.unit { state =>
          state.updateMetaJournal(key, segment) { entry =>
            entry.copy(
              journalHead = entry.journalHead.copy(partitionOffset = partitionOffset, deleteTo = deleteTo.some),
              updated     = timestamp,
            )
          }
        }
      }
  }

  val selectOffset: PointerStatements.SelectOffset[StateT] = { (topic: Topic, partition: Partition) =>
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

  val insertPointer: PointerStatements.Insert[StateT] = {
    (topic: Topic, partition: Partition, offset: Offset, created: Instant, updated: Instant) =>
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

  val updatePointer: PointerStatements.Update[StateT] = {
    (topic: Topic, partition: Partition, offset: Offset, timestamp: Instant) =>
      {
        StateT.unit { state =>
          state.updatePointer(topic, partition) { entry =>
            entry.copy(offset = offset, updated = timestamp)
          }
        }
      }
  }

  val selectTopics: PointerStatements.SelectTopics[StateT] = { () =>
    {
      StateT.success { state =>
        val topics = state.pointers.keySet.toSortedSet
        (state, topics)
      }
    }
  }

  val selectRecords: JournalStatements.SelectRecords[StateT] = { (key: Key, segment: SegmentNr, range: SeqRange) =>
    {
      val stateT = StateT.success { state =>
        val entries = for {
          journal <- state.journal.get((key, segment)).toList
          record  <- journal.collect { case (_, entry) if entry.seqNr in range => entry }.toList
        } yield {
          record
        }
        val stream = Stream[StateT].apply(entries)
        (state, stream)
      }
      stateT.toStream.flatten
    }
  }

  def statementsOf(segmentNrsOf: SegmentNrsOf[StateT], segments: Segments): EventualCassandra.Statements[StateT] = {
    val concurrentStateT: Concurrent[StateT] = ConcurrentOf.fromMonad[StateT]

    val metaJournalStatements = EventualCassandra
      .MetaJournalStatements
      .fromMetaJournal(
        segmentNrsOf   = segmentNrsOf,
        journalHead    = selectJournalHead0,
        journalPointer = selectJournalPointer0,
        ids            = selectIds0,
        segments       = segments,
      )(concurrentStateT)

    EventualCassandra.Statements(selectRecords, metaJournalStatements, selectOffset, selectOffset2)
  }

  final case class PointerEntry(offset: Offset, created: Instant, updated: Instant)

  final case class State(
    pointers: Map[Topic, Map[Partition, PointerEntry]]                                         = Map.empty,
    metadata: Map[Topic, Map[String, MetaJournalEntry]]                                        = Map.empty,
    metaJournal: Map[(Topic, SegmentNr), Map[String, MetaJournalEntry]]                        = Map.empty,
    journal: Map[(Key, SegmentNr), Map[(SeqNr, Instant), EventRecord[EventualPayloadAndType]]] = Map.empty,
  )

  object State {

    val empty: State = State()

    implicit class StateOps(val self: State) extends AnyVal {

      def updateMetadata(key: Key)(f: MetaJournalEntry => MetaJournalEntry): State = {
        val state = for {
          entries <- self.metadata.get(key.topic)
          entry   <- entries.get(key.id)
        } yield {
          val entry1   = f(entry)
          val entries1 = entries.updated(key.id, entry1)
          self.copy(metadata = self.metadata.updated(key.topic, entries1))
        }
        state getOrElse self
      }

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
