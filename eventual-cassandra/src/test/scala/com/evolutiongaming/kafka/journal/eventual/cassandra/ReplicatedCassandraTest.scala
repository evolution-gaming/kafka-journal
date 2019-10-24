package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.Instant

import cats.Parallel
import cats.data.{IndexedStateT, NonEmptyList => Nel}
import cats.effect.ExitCase
import cats.implicits._
import com.evolutiongaming.catshelper.BracketThrowable
import com.evolutiongaming.catshelper.NelHelper._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.TopicPointers
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
      metadata = Metadata(Json.obj(("key", "value")).some),
      headers = Headers(("key", "value")))
  }

  for {
    segmentSize <- List(SegmentSize.min, SegmentSize.default, SegmentSize.max)
  } {

    val journal = ReplicatedCassandra(segmentSize, statements)

    test(s"topics, segmentSize: $segmentSize") {
      val id = "id"
      val key = Key(id = id, topic = topic0)
      val stateT = for {
        topics <- journal.topics
        _       = topics.toSet shouldEqual Set.empty
        _      <- journal.append(key, partitionOffset, timestamp0, Nel.of(record))
        topics <- journal.topics
        _       = topics.toSet shouldEqual Set.empty
        _      <- journal.save(topic0, TopicPointers(Map((0, 0))), timestamp0)
        topics <- journal.topics
        _       = topics.toSet shouldEqual Set(topic0)
        _      <- journal.save(topic0, TopicPointers(Map((0, 1))), timestamp1)
        topics <- journal.topics
        _       = topics.toSet shouldEqual Set(topic0)
        _      <- journal.save(topic1, TopicPointers(Map((0, 0))), timestamp0)
        topics <- journal.topics
        _       = topics.toSet shouldEqual Set(topic0, topic1)
        _      <- journal.delete(key, partitionOffset, timestamp1, SeqNr.max, origin.some)
        topics <- journal.topics
        _       = topics.toSet shouldEqual Set(topic0, topic1)
      } yield {}

      val expected = State(
        pointers = Map(
          (topic0, Map((0, PointerEntry(offset = 1, created = timestamp0, updated = timestamp1)))),
          (topic1, Map((0, PointerEntry(offset = 0, created = timestamp0, updated = timestamp0))))),
        metadata = Map(
          (topic0, Map((id, MetadataEntry(
            partitionOffset = partitionOffset,
            segmentSize = segmentSize,
            seqNr = SeqNr.max,
            deleteTo = SeqNr.max.some,
            created = timestamp0,
            updated = timestamp1,
            origin = origin.some))))),
        journal = Map(((key, SegmentNr.min), Map(((SeqNr.min, timestamp0), record)))))
      val result = stateT.run(State.empty)
      result shouldEqual (expected, ()).pure[Try]
    }


    test(s"pointers, segmentSize: $segmentSize") {
      val id = "id"
      val key = Key(id = id, topic = topic0)
      val stateT = for {
        pointers <- journal.pointers(topic0)
        _         = pointers.values shouldEqual Map.empty
        _        <- journal.append(key, partitionOffset, timestamp0, Nel.of(record))
        pointers <- journal.pointers(topic0)
        _         = pointers.values shouldEqual Map.empty
        _        <- journal.save(topic0, TopicPointers(Map((0, 0))), timestamp0)
        pointers <- journal.pointers(topic0)
        _         = pointers.values shouldEqual Map((0, 0))
        _        <- journal.save(topic0, TopicPointers(Map((0, 1))), timestamp1)
        pointers <- journal.pointers(topic0)
        _         = pointers.values shouldEqual Map((0, 1))
        _        <- journal.save(topic1, TopicPointers(Map((0, 0))), timestamp0)
        pointers <- journal.pointers(topic0)
        _         = pointers.values shouldEqual Map((0, 1))
      } yield {}

      val expected = State(
        pointers = Map(
          (topic0, Map((0, PointerEntry(offset = 1, created = timestamp0, updated = timestamp1)))),
          (topic1, Map((0, PointerEntry(offset = 0, created = timestamp0, updated = timestamp0))))),
        metadata = Map(
          (topic0, Map((id, MetadataEntry(partitionOffset, segmentSize, SeqNr.min, none, timestamp0, timestamp0, origin.some))))),
        journal = Map(((key, SegmentNr.min), Map(((SeqNr.min, timestamp0), record)))))
      val result = stateT.run(State.empty)
      result shouldEqual (expected, ()).pure[Try]
    }


    test(s"append, segmentSize: $segmentSize") {
      val id0 = "id0"
      val id1 = "id1"
      val key0 = Key(id0, topic0)
      val key1 = Key(id1, topic1)
      val stateT = for {
        _ <- journal.append(
          key = key0,
          partitionOffset = PartitionOffset(partition = 0, offset = 0),
          timestamp = timestamp0,
          events = Nel.of(
            eventRecordOf(
              seqNr = SeqNr.unsafe(1),
              partitionOffset = PartitionOffset(partition = 0, offset = 0))))
        _ <- journal.append(
          key = key0,
          partitionOffset = PartitionOffset(partition = 0, offset = 3),
          timestamp = timestamp1,
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
        metadata = Map(
          (topic0, Map(
            (id0, MetadataEntry(
              PartitionOffset(partition = 0, offset = 3),
              segmentSize,
              SeqNr.unsafe(3),
              none,
              timestamp0,
              timestamp1,
              origin.some)))),
          (topic1, Map(
            (id1, MetadataEntry(
              PartitionOffset(partition = 0, offset = 4),
              segmentSize,
              SeqNr.unsafe(1),
              none,
              timestamp0,
              timestamp0,
              origin.some))))),
        journal = events0 ++ Map(
          ((key1, SegmentNr.min), Map(
            ((SeqNr.min, timestamp0), eventRecordOf(
              seqNr = SeqNr.unsafe(1),
              partitionOffset = PartitionOffset(partition = 0, offset = 4)))))))
      val result = stateT.run(State.empty)
      result shouldEqual (expected, ()).pure[Try]
    }


    test(s"delete, segmentSize: $segmentSize") {
      val id = "id"
      val key = Key(id = id, topic = topic0)
      val stateT = for {
        _ <- journal.append(
          key = key,
          partitionOffset = PartitionOffset(partition = 0, offset = 1),
          timestamp = timestamp0,
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
        metadata = Map(
          (topic0, Map((id, MetadataEntry(
            PartitionOffset(partition = 0, offset = 2),
            segmentSize,
            SeqNr.max,
            SeqNr.max.some,
            timestamp0,
            timestamp1,
            origin.some))))),
        journal = Map(((key, SegmentNr.min), Map(((SeqNr.min, timestamp0), record)))))
      val result = stateT.run(State.empty)
      result shouldEqual (expected, ()).pure[Try]
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


  val insertMetadata: MetadataStatements.Insert[StateT] = {
    (key: Key, timestamp: Instant, head: JournalHead, origin: Option[Origin]) => {
      StateT.unit { state =>
        val entry = MetadataEntry(
          partitionOffset = head.partitionOffset,
          segmentSize = head.segmentSize,
          seqNr = head.seqNr,
          deleteTo = head.deleteTo,
          created = timestamp,
          updated = timestamp,
          origin = origin)
        val entries = state
          .metadata
          .getOrElse(key.topic, Map.empty)
          .updated(key.id, entry)
        state.copy(metadata = state.metadata.updated(key.topic, entries))
      }
    }
  }


  val selectJournalHead: MetadataStatements.SelectJournalHead[StateT] = {
    key: Key => {
      StateT.success { state =>
        val head = for {
          entries <- state.metadata.get(key.topic)
          entry   <- entries.get(key.id)
        } yield {
          JournalHead(
            partitionOffset = entry.partitionOffset,
            segmentSize = entry.segmentSize,
            seqNr = entry.seqNr,
            deleteTo = entry.deleteTo)
        }
        (state, head)
      }
    }
  }


  val updateMetadata: MetadataStatements.Update[StateT] = {
    (key: Key, partitionOffset: PartitionOffset, timestamp: Instant, seqNr: SeqNr, deleteTo: SeqNr) => {
      StateT.unit { state =>
        state.updateMetadata(key) { entry =>
          entry.copy(
            partitionOffset = partitionOffset,
            updated = timestamp,
            seqNr = seqNr,
            deleteTo = deleteTo.some)
        }
      }
    }
  }


  val updateMetadataSeqNr: MetadataStatements.UpdateSeqNr[StateT] = {
    (key: Key, partitionOffset: PartitionOffset, timestamp: Instant, seqNr: SeqNr) => {
      StateT.unit { state =>
        state.updateMetadata(key) { entry =>
          entry.copy(
            partitionOffset = partitionOffset,
            updated = timestamp,
            seqNr = seqNr)
        }
      }
    }
  }


  val updateHeadDeleteTo: MetadataStatements.UpdateDeleteTo[StateT] = {
    (key: Key, partitionOffset: PartitionOffset, timestamp: Instant, deleteTo: SeqNr) => {
      StateT.unit { state =>
        state.updateMetadata(key) { entry =>
          entry.copy(
            partitionOffset = partitionOffset,
            updated = timestamp,
            deleteTo = deleteTo.some)
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

    val metadata = ReplicatedCassandra.MetaJournalStatements(
      selectJournalHead,
      insertMetadata,
      updateMetadata,
      updateMetadataSeqNr,
      updateHeadDeleteTo)

    ReplicatedCassandra.Statements(
      insertRecords,
      deleteRecords,
      metadata,
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

  final case class MetadataEntry(
    partitionOffset: PartitionOffset,
    segmentSize: SegmentSize,
    seqNr: SeqNr,
    deleteTo: Option[SeqNr],
    created: Instant,
    updated: Instant,
    origin: Option[Origin])


  final case class PointerEntry(
    offset: Offset,
    created: Instant,
    updated: Instant)


  final case class State(
    pointers: Map[Topic, Map[Partition, PointerEntry]] = Map.empty,
    metadata: Map[Topic, Map[String, MetadataEntry]] = Map.empty,
    journal: Map[(Key, SegmentNr), Map[(SeqNr, Instant), EventRecord]] = Map.empty)

  object State {

    val empty: State = State()


    implicit class StateOps(val self: State) extends AnyVal {

      def updateMetadata(key: Key)(f: MetadataEntry => MetadataEntry): State = {
        val state = for {
          entries <- self.metadata.get(key.topic)
          entry   <- entries.get(key.id)
        } yield {
          val entry1 = f(entry)
          val entries1 = entries.updated(key.id, entry1)
          self.copy(metadata = self.metadata.updated(key.topic, entries1))
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
