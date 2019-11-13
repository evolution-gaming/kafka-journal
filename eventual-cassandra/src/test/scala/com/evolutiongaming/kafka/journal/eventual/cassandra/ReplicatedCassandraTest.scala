package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.Instant

import cats.data.{IndexedStateT, NonEmptyList => Nel, NonEmptyMap => Nem}
import cats.effect.ExitCase
import cats.implicits._
import cats.{Id, Parallel}
import com.evolutiongaming.catshelper.BracketThrowable
import com.evolutiongaming.catshelper.NelHelper._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.util.BracketFromMonadError
import com.evolutiongaming.kafka.journal.util.TemporalHelper._
import com.evolutiongaming.skafka.{Offset, Partition, Topic}
import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.Json

import scala.concurrent.duration._
import scala.util.Try

class ReplicatedCassandraTest extends FunSuite with Matchers {
  import ReplicatedCassandraTest._

  private val timestamp0 = Instant.now()
  private val timestamp1 = timestamp0 + 1.minute
  private val topic0 = "topic0"
  private val topic1 = "topic1"
  private val partitionOffset = PartitionOffset.empty
  private val origin = Origin("origin")
  private val record = eventRecordOf(SeqNr.min, partitionOffset)

  private def eventRecordOf(seqNr: SeqNr, partitionOffset: PartitionOffset) = {
    EventRecord(
      event = Event(seqNr),
      timestamp = timestamp0,
      partitionOffset = partitionOffset,
      origin = origin.some,
      metadata = RecordMetadata(Json.obj(("key", "value")).some),
      headers = Headers(("key", "value")))
  }

  for {
    segmentSize <- List(SegmentSize.min, SegmentSize.default, SegmentSize.max)
    segments    <- List(Segments.min, Segments.default)
  } {

    val segmentOfId = SegmentOf[Id](segments)
    val segmentOf = SegmentOf[StateT](segments)
    val journal = ReplicatedCassandra(segmentSize, segmentOf, statements)

    val suffix = s"segmentSize: $segmentSize, segments: $segments"

    test(s"topics, $suffix") {
      val id = "id"
      val key = Key(id = id, topic = topic0)
      val segment = segmentOfId(key)
      val partitionOffset1 = partitionOffset.copy(offset = partitionOffset.offset + 1)

      val stateT = for {
        topics <- journal.topics
        _       = topics.toSet shouldEqual Set.empty
        _      <- journal.append(key, partitionOffset, timestamp0, none, Nel.of(record))
        topics <- journal.topics
        _       = topics.toSet shouldEqual Set.empty
        _      <- journal.save(topic0, Nem.of((0, 0)), timestamp0)
        topics <- journal.topics
        _       = topics.toSet shouldEqual Set(topic0)
        _      <- journal.save(topic0, Nem.of((0, 1)), timestamp1)
        topics <- journal.topics
        _       = topics.toSet shouldEqual Set(topic0)
        _      <- journal.save(topic1, Nem.of((0, 0)), timestamp0)
        topics <- journal.topics
        _       = topics.toSet shouldEqual Set(topic0, topic1)
        _      <- journal.delete(key, partitionOffset1, timestamp1, SeqNr.max, origin.some)
        topics <- journal.topics
        _       = topics.toSet shouldEqual Set(topic0, topic1)
      } yield {}

      val expected = State(
        pointers = Map(
          (topic0, Map((0, PointerEntry(offset = 1, created = timestamp0, updated = timestamp1)))),
          (topic1, Map((0, PointerEntry(offset = 0, created = timestamp0, updated = timestamp0))))),
        metaJournal = Map(
          ((topic0, segment), Map((id, MetaJournalEntry(
            journalHead = JournalHead(
              partitionOffset = partitionOffset1,
              segmentSize = segmentSize,
              seqNr = SeqNr.max,
              deleteTo = SeqNr.max.some),
            created = timestamp0,
            updated = timestamp1,
            origin = origin.some))))),
        journal = Map(((key, SegmentNr.min), Map(((SeqNr.min, timestamp0), record)))))
      val result = stateT.run(State.empty)
      result shouldEqual (expected, ()).pure[Try]
    }


    test(s"pointers, $suffix") {
      val id = "id"
      val key = Key(id = id, topic = topic0)
      val segment = segmentOfId(key)
      val stateT = for {
        pointers <- journal.pointers(topic0)
        _         = pointers.values shouldEqual Map.empty
        _        <- journal.append(key, partitionOffset, timestamp0, expireAfter = none, Nel.of(record))
        pointers <- journal.pointers(topic0)
        _         = pointers.values shouldEqual Map.empty
        _        <- journal.save(topic0, Nem.of((0, 0)), timestamp0)
        pointers <- journal.pointers(topic0)
        _         = pointers.values shouldEqual Map((0, 0))
        _        <- journal.save(topic0, Nem.of((0, 1)), timestamp1)
        pointers <- journal.pointers(topic0)
        _         = pointers.values shouldEqual Map((0, 1))
        _        <- journal.save(topic1, Nem.of((0, 0)), timestamp0)
        pointers <- journal.pointers(topic0)
        _         = pointers.values shouldEqual Map((0, 1))
      } yield {}

      val expected = State(
        pointers = Map(
          (topic0, Map((0, PointerEntry(offset = 1, created = timestamp0, updated = timestamp1)))),
          (topic1, Map((0, PointerEntry(offset = 0, created = timestamp0, updated = timestamp0))))),
        metaJournal = Map(
          ((topic0, segment), Map((id, MetaJournalEntry(
            journalHead = JournalHead(
              partitionOffset = partitionOffset,
              segmentSize = segmentSize,
              seqNr = SeqNr.min,
              deleteTo = none),
            created = timestamp0,
            updated = timestamp0,
            origin = origin.some))))),
        journal = Map(((key, SegmentNr.min), Map(((SeqNr.min, timestamp0), record)))))
      val result = stateT.run(State.empty)
      result shouldEqual (expected, ()).pure[Try]
    }


    test(s"append, $suffix") {
      val id0 = "id0"
      val id1 = "id1"
      val key0 = Key(id0, topic0)
      val key1 = Key(id1, topic1)
      val segment0 = segmentOfId(key0)
      val segment1 = segmentOfId(key1)
      val stateT = for {
        _ <- journal.append(
          key = key0,
          partitionOffset = PartitionOffset(partition = 0, offset = 0),
          timestamp = timestamp0,
          expireAfter = 1.minute.some,
          events = Nel.of(
            eventRecordOf(
              seqNr = SeqNr.unsafe(1),
              partitionOffset = PartitionOffset(partition = 0, offset = 0))))
        _ <- journal.append(
          key = key0,
          partitionOffset = PartitionOffset(partition = 0, offset = 3),
          timestamp = timestamp1,
          expireAfter = none,
          events = Nel.of(
            eventRecordOf(
              seqNr = SeqNr.unsafe(2),
              partitionOffset = PartitionOffset(partition = 0, offset = 1)),
            eventRecordOf(
              seqNr = SeqNr.unsafe(3),
              partitionOffset = PartitionOffset(partition = 0, offset = 2))))
        _ <- journal.append(
          key = key1,
          partitionOffset = PartitionOffset(partition = 0, offset = 4),
          timestamp = timestamp0,
          expireAfter = none,
          events = Nel.of(
            eventRecordOf(
              seqNr = SeqNr.unsafe(1),
              partitionOffset = PartitionOffset(partition = 0, offset = 4))))
      } yield {}


      val events0 = Nel
        .of(
          eventRecordOf(
            seqNr = SeqNr.unsafe(1),
            partitionOffset = PartitionOffset(partition = 0, offset = 0)),
          eventRecordOf(
            seqNr = SeqNr.unsafe(2),
            partitionOffset = PartitionOffset(partition = 0, offset = 1)),
          eventRecordOf(
            seqNr = SeqNr.unsafe(3),
            partitionOffset = PartitionOffset(partition = 0, offset = 2)))
        .grouped(segmentSize.value)
        .zipWithIndex
        .map { case (events, segmentNr) =>
          val map = events
            .map { a => ((a.seqNr, a.timestamp), a) }
            .toList
            .toMap
          ((key0, SegmentNr.unsafe(segmentNr)), map)
        }
        .toList
        .toMap

      val expected = State(
        metaJournal = Map(
          ((topic0, segment0), Map(
            (id0, MetaJournalEntry(
              journalHead = JournalHead(
                partitionOffset = PartitionOffset(partition = 0, offset = 3),
                segmentSize = segmentSize,
                seqNr = SeqNr.unsafe(3),
                deleteTo = none),
              created = timestamp0,
              updated = timestamp1,
              origin = origin.some)))),
          ((topic1, segment1), Map(
            (id1, MetaJournalEntry(
              journalHead = JournalHead(
                partitionOffset = PartitionOffset(partition = 0, offset = 4),
                segmentSize = segmentSize,
                seqNr = SeqNr.unsafe(1),
                deleteTo = none),
              created = timestamp0,
              updated = timestamp0,
              origin = origin.some))))),
        journal = events0 ++ Map(
          ((key1, SegmentNr.min), Map(
            ((SeqNr.min, timestamp0), eventRecordOf(
              seqNr = SeqNr.unsafe(1),
              partitionOffset = PartitionOffset(partition = 0, offset = 4)))))))
      val result = stateT.run(State.empty)
      result shouldEqual (expected, ()).pure[Try]
    }


    /*TODO expireAfter: test*/
    test(s"append & override expireAfter, $suffix") {
      val id = "id"
      val key = Key(id, topic0)
      val segment = segmentOfId(key)
      val stateT = for {
        _ <- journal.append(
          key = key,
          partitionOffset = PartitionOffset(partition = 0, offset = 0),
          timestamp = timestamp0,
          expireAfter = 1.minute.some,
          events = Nel.of(
            eventRecordOf(
              seqNr = SeqNr.unsafe(1),
              partitionOffset = PartitionOffset(partition = 0, offset = 0))))
        _ <- journal.append(
          key = key,
          partitionOffset = PartitionOffset(partition = 0, offset = 3),
          timestamp = timestamp1,
          expireAfter = 2.minutes.some,
          events = Nel.of(
            eventRecordOf(
              seqNr = SeqNr.unsafe(2),
              partitionOffset = PartitionOffset(partition = 0, offset = 1)),
            eventRecordOf(
              seqNr = SeqNr.unsafe(3),
              partitionOffset = PartitionOffset(partition = 0, offset = 2))))
      } yield {}


      val events0 = Nel
        .of(
          eventRecordOf(
            seqNr = SeqNr.unsafe(1),
            partitionOffset = PartitionOffset(partition = 0, offset = 0)),
          eventRecordOf(
            seqNr = SeqNr.unsafe(2),
            partitionOffset = PartitionOffset(partition = 0, offset = 1)),
          eventRecordOf(
            seqNr = SeqNr.unsafe(3),
            partitionOffset = PartitionOffset(partition = 0, offset = 2)))
        .grouped(segmentSize.value)
        .zipWithIndex
        .map { case (events, segmentNr) =>
          val map = events
            .map { a => ((a.seqNr, a.timestamp), a) }
            .toList
            .toMap
          ((key, SegmentNr.unsafe(segmentNr)), map)
        }
        .toList
        .toMap

      val expected = State(
        metaJournal = Map(
          ((topic0, segment), Map(
            (id, MetaJournalEntry(
              journalHead = JournalHead(
                partitionOffset = PartitionOffset(partition = 0, offset = 3),
                segmentSize = segmentSize,
                seqNr = SeqNr.unsafe(3),
                deleteTo = none),
              created = timestamp0,
              updated = timestamp1,
              origin = origin.some))))),
        journal = events0)
      val result = stateT.run(State.empty)
      result shouldEqual (expected, ()).pure[Try]
    }


    test(s"do not repeat appends, $suffix") {
      val id = "id"
      val key = Key(id, topic0)
      val segment = segmentOfId(key)
      val stateT = journal.append(
        key = key,
        partitionOffset = PartitionOffset(partition = 0, offset = 4),
        timestamp = timestamp1,
        expireAfter = none,
        events = Nel.of(
          eventRecordOf(
            seqNr = SeqNr.unsafe(1),
            partitionOffset = PartitionOffset(partition = 0, offset = 1)),
          eventRecordOf(
            seqNr = SeqNr.unsafe(2),
            partitionOffset = PartitionOffset(partition = 0, offset = 3))))

      val expected = State(
        metaJournal = Map(
          ((topic0, segment), Map(
            (id, MetaJournalEntry(
              journalHead = JournalHead(
                partitionOffset = PartitionOffset(partition = 0, offset = 4),
                segmentSize = segmentSize,
                seqNr = SeqNr.unsafe(2),
                deleteTo = none),
              created = timestamp0,
              updated = timestamp1,
              origin = origin.some))))),
        journal = Map((
          (key, SegmentNr.min),
          Map((
            (SeqNr.unsafe(2), timestamp0),
            eventRecordOf(
              seqNr = SeqNr.unsafe(2),
              partitionOffset = PartitionOffset(partition = 0, offset = 3)))))))

      val initial = State.empty.copy(
        metaJournal = Map(
          ((topic0, segment), Map(
            (id, MetaJournalEntry(
              journalHead = JournalHead(
                partitionOffset = PartitionOffset(partition = 0, offset = 2),
                segmentSize = segmentSize,
                seqNr = SeqNr.unsafe(1),
                deleteTo = none),
              created = timestamp0,
              updated = timestamp0,
              origin = origin.some))))))

      val actual = stateT.run(initial)
      actual shouldEqual (expected, ()).pure[Try]
    }


    test(s"delete, $suffix") {
      val id = "id"
      val key = Key(id = id, topic = topic0)
      val segment = segmentOfId(key)
      val stateT = for {
        _ <- journal.append(
          key = key,
          partitionOffset = PartitionOffset(partition = 0, offset = 1),
          timestamp = timestamp0,
          expireAfter = none,
          events = Nel.of(
            eventRecordOf(
              seqNr = SeqNr.min,
              partitionOffset = PartitionOffset(partition = 0, offset = 0))))
        _ <- journal.delete(
          key = key,
          partitionOffset = PartitionOffset(partition = 0, offset = 2),
          timestamp = timestamp1,
          deleteTo = SeqNr.max,
          origin = origin.some)
      } yield {}

      val expected = State(
        metaJournal = Map(
          ((topic0, segment), Map((id, MetaJournalEntry(
            journalHead = JournalHead(
              partitionOffset = PartitionOffset(partition = 0, offset = 2),
              segmentSize = segmentSize,
              seqNr = SeqNr.max,
              deleteTo = SeqNr.max.some),
            created = timestamp0,
            updated = timestamp1,
            origin = origin.some))))),
        journal = Map(((key, SegmentNr.min), Map(((SeqNr.min, timestamp0), record)))))
      val result = stateT.run(State.empty)
      result shouldEqual (expected, ()).pure[Try]
    }


    test(s"do not repeat deletions, $suffix") {
      val id = "id"
      val key = Key(id = id, topic = topic0)
      val segment = segmentOfId(key)
      val stateT = journal.delete(
        key = key,
        partitionOffset = PartitionOffset(partition = 0, offset = 1),
        timestamp = timestamp1,
        deleteTo = SeqNr.min,
        origin = origin.some)

      val initial = State.empty.copy(
        metaJournal = Map(
          ((topic0, segment), Map(
            (id, MetaJournalEntry(
              journalHead = JournalHead(
                partitionOffset = PartitionOffset(partition = 0, offset = 2),
                segmentSize = segmentSize,
                seqNr = SeqNr.min,
                deleteTo = SeqNr.min.some),
              created = timestamp0,
              updated = timestamp0,
              origin = origin.some))))),
        journal = Map(((key, SegmentNr.min), Map(((SeqNr.min, timestamp0), record)))))

      val expected = State(
        metaJournal = Map(
          ((topic0, segment), Map((id, MetaJournalEntry(
            journalHead = JournalHead(
              partitionOffset = PartitionOffset(partition = 0, offset = 2),
              segmentSize = segmentSize,
              seqNr = SeqNr.min,
              deleteTo = SeqNr.min.some),
            created = timestamp0,
            updated = timestamp0,
            origin = origin.some))))),
        journal = Map(((key, SegmentNr.min), Map(((SeqNr.min, timestamp0), record)))))

      val actual = stateT.run(initial)
      actual shouldEqual (expected, ()).pure[Try]
    }
  }
}

object ReplicatedCassandraTest {

  val insertRecords: JournalStatements.InsertRecords[StateT] = {
    (key: Key, segment: SegmentNr, events: Nel[EventRecord]) => {
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


  val deleteRecords: JournalStatements.DeleteRecords[StateT] = {
    (key: Key, segment: SegmentNr, seqNr: SeqNr) => {
      StateT.unit { state =>
        val k = (key, segment)
        val journal = state.journal
        val entries = journal
          .getOrElse(k, Map.empty)
          .filterKeys { case (a, _) => a <= seqNr }
        val journal1 = if (entries.isEmpty) journal - k else journal.updated(k, entries)
        state.copy(journal = journal1)
      }
    }
  }


  val insertMetaJournal: MetaJournalStatements.Insert[StateT] = {
    (key: Key, segment: SegmentNr, created: Instant, updated: Instant, journalHead: JournalHead, origin: Option[Origin]) => {
      StateT.unit { state =>
        val entry = MetaJournalEntry(
          journalHead = journalHead,
          created = created,
          updated = updated,
          origin = origin)
        val entries = state
          .metaJournal
          .getOrElse((key.topic, segment), Map.empty)
          .updated(key.id, entry)
        state.copy(metaJournal = state.metaJournal.updated((key.topic, segment), entries))
      }
    }
  }


  val selectMetaJournalJournalHead: MetaJournalStatements.SelectJournalHead[StateT] = {
    (key: Key, segment: SegmentNr) => {
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


  val updateMetaJournal: MetaJournalStatements.Update[StateT] = {
    (key: Key, segment: SegmentNr, partitionOffset: PartitionOffset, timestamp: Instant, seqNr: SeqNr, deleteTo: SeqNr) => {
      StateT.unit { state =>
        state.updateMetaJournal(key, segment) { entry =>
          entry.copy(
            journalHead = entry.journalHead.copy(
              partitionOffset = partitionOffset,
              seqNr = seqNr,
              deleteTo = deleteTo.some),
            updated = timestamp)
        }
      }
    }
  }


  val updateMetaJournalSeqNr: MetaJournalStatements.UpdateSeqNr[StateT] = {
    (key: Key, segment: SegmentNr, partitionOffset: PartitionOffset, timestamp: Instant, seqNr: SeqNr) => {
      StateT.unit { state =>
        state.updateMetaJournal(key, segment) { entry =>
          entry.copy(
            journalHead = entry.journalHead.copy(
              partitionOffset = partitionOffset,
              seqNr = seqNr),
            updated = timestamp)
        }
      }
    }
  }


  val updateMetaJournalDeleteTo: MetaJournalStatements.UpdateDeleteTo[StateT] = {
    (key: Key, segment: SegmentNr, partitionOffset: PartitionOffset, timestamp: Instant, deleteTo: SeqNr) => {
      StateT.unit { state =>
        state.updateMetaJournal(key, segment) { entry =>
          entry.copy(
            journalHead = entry.journalHead.copy(
              partitionOffset = partitionOffset,
              deleteTo = deleteTo.some),
            updated = timestamp)
        }
      }
    }
  }

  val selectPointer: PointerStatements.Select[StateT] = {
    (topic: Topic, partition: Partition) => {
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


  val selectPointersIn: PointerStatements.SelectIn[StateT] = {
    (topic: Topic, partitions: Nel[Partition]) => {
      StateT.success { state =>
        val pointers = state
          .pointers
          .getOrElse(topic, Map.empty)
        val offsets = for {
          partition <- partitions.toList
          pointer   <- pointers.get(partition)
        } yield {
          (partition, pointer.offset)
        }
        (state, offsets.toMap)
      }
    }
  }


  val selectPointers: PointerStatements.SelectAll[StateT] = {
    topic: Topic => {
      StateT.success { state =>
        val offsets = state
          .pointers
          .getOrElse(topic, Map.empty)
          .map { case (partition, entry) => (partition, entry.offset) }
        (state, offsets)
      }
    }
  }


  val insertPointer: PointerStatements.Insert[StateT] = {
    (topic: Topic, partition: Partition, offset: Offset, created: Instant, updated: Instant) => {
      StateT.unit { state =>
        val entry = PointerEntry(
          offset = offset,
          created = created,
          updated = updated)
        val entries = state
          .pointers
          .getOrElse(topic, Map.empty)
          .updated(partition, entry)
        state.copy(pointers = state.pointers.updated(topic, entries))
      }
    }
  }


  val updatePointer: PointerStatements.Update[StateT] = {
    (topic: Topic, partition: Partition, offset: Offset, timestamp: Instant) => {
      StateT.unit { state =>
        state.updatePointer(topic, partition) { entry =>
          entry.copy(
            offset = offset,
            updated = timestamp)
        }
      }
    }
  }


  val selectTopics: PointerStatements.SelectTopics[StateT] = {
    () => {
      StateT.success { state =>
        val topics = state.pointers.keys.toList
        (state, topics)
      }
    }
  }


  val statements: ReplicatedCassandra.Statements[StateT] = {

    val metaJournal = ReplicatedCassandra.MetaJournalStatements(
      selectMetaJournalJournalHead,
      insertMetaJournal,
      updateMetaJournal,
      updateMetaJournalSeqNr,
      updateMetaJournalDeleteTo)

    ReplicatedCassandra.Statements(
      insertRecords,
      deleteRecords,
      metaJournal,
      selectPointer,
      selectPointersIn,
      selectPointers,
      insertPointer,
      updatePointer,
      selectTopics)
  }


  implicit val bracket: BracketThrowable[StateT] = new BracketFromMonadError[StateT, Throwable] {

    val F = IndexedStateT.catsDataMonadErrorForIndexedStateT(catsStdInstancesForTry)

    def bracketCase[A, B](
      acquire: StateT[A])(
      use: A => StateT[B])(
      release: (A, ExitCase[Throwable]) => StateT[Unit]
    ) = {

      def onError(a: A)(e: Throwable) = for {
        _ <- release(a, ExitCase.error(e))
        b <- raiseError[B](e)
      } yield b

      for {
        a <- acquire
        b <- handleErrorWith(use(a))(onError(a))
        _ <- release(a, ExitCase.complete)
      } yield b
    }
  }

  implicit val parallel: Parallel[StateT] = Parallel.identity[StateT]


  final case class PointerEntry(
    offset: Offset,
    created: Instant,
    updated: Instant)


  final case class State(
    pointers: Map[Topic, Map[Partition, PointerEntry]] = Map.empty,
    metaJournal: Map[(Topic, SegmentNr), Map[String, MetaJournalEntry]] = Map.empty,
    journal: Map[(Key, SegmentNr), Map[(SeqNr, Instant), EventRecord]] = Map.empty)

  object State {

    val empty: State = State()


    implicit class StateOps(val self: State) extends AnyVal {

      def updateMetaJournal(key: Key, segment: SegmentNr)(f: MetaJournalEntry => MetaJournalEntry): State = {
        val state = for {
          entries <- self.metaJournal.get((key.topic, segment))
          entry   <- entries.get(key.id)
        } yield {
          val entry1 = f(entry)
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
          val entry1 = f(entry)
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
