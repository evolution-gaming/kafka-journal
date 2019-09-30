package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.Instant

import cats.Parallel
import cats.data.{IndexedStateT, NonEmptyList => Nel}
import cats.effect.ExitCase
import cats.implicits._
import com.evolutiongaming.catshelper.BracketThrowable
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.TopicPointers
import com.evolutiongaming.kafka.journal.util.BracketFromMonadError
import com.evolutiongaming.kafka.journal.util.TimeHelper._
import com.evolutiongaming.skafka.{Offset, Partition, Topic}
import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.Json

import scala.concurrent.duration._
import scala.util.Try

class ReplicatedCassandraTest extends FunSuite with Matchers {
  import ReplicatedCassandraTest._

  for {
    segmentSize <- List(2, 5, 10)
  } {
    test(s"topics, segmentSize: $segmentSize") {
      val journal = ReplicatedCassandra(segmentSize, statements)
      val timestamp0 = Instant.now()
      val timestamp1 = timestamp0 + 1.minute
      val topic0 = "topic0"
      val topic1 = "topic1"
      val partitionOffset = PartitionOffset.empty
      val origin = Origin("origin")
      val record = EventRecord(
        event = Event(SeqNr.min),
        timestamp = timestamp0,
        partitionOffset = partitionOffset,
        origin = origin.some,
        metadata = Metadata(Json.obj(("key", "value")).some),
        headers = Headers(("key", "value")))
      val id = "id"
      val key = Key(id = id, topic = topic0)
      val stateT = for {
        topics   <- journal.topics
        _         = topics shouldEqual List.empty
        _        <- journal.append(key, partitionOffset, timestamp0, Nel.of(record))
        topics   <- journal.topics
        _         = topics.toSet shouldEqual Set.empty
        _        <- journal.save(topic0, TopicPointers(Map((0, 0))), timestamp0)
        topics   <- journal.topics
        _         = topics.toSet shouldEqual Set(topic0)
        _        <- journal.save(topic0, TopicPointers(Map((0, 1))), timestamp1)
        topics   <- journal.topics
        _         = topics.toSet shouldEqual Set(topic0)
        _        <- journal.save(topic1, TopicPointers(Map((0, 0))), timestamp0)
        topics   <- journal.topics
        _         = topics.toSet shouldEqual Set(topic0, topic1)
      } yield {}

      val expected = State(
        pointers = Map(
          (topic0, Map((0, PointerEntry(offset = 1, created = timestamp0, updated = timestamp1)))),
          (topic1, Map((0, PointerEntry(offset = 0, created = timestamp0, updated = timestamp0))))),
        head = Map(
          (topic0, Map((id, HeadEntry(partitionOffset, segmentSize, SeqNr.min, none, timestamp0, timestamp0, origin.some))))),
        journal = Map(((key, SegmentNr(0)), Map(((SeqNr.min, timestamp0), record)))))
      val result = stateT.run(State.empty)
      result shouldEqual (expected, ()).pure[Try]
    }
  }
}

object ReplicatedCassandraTest {

  val insertRecords: JournalStatement.InsertRecords[StateT] = {
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


  val deleteRecords: JournalStatement.DeleteRecords[StateT] = {
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


  val insertHead: HeadStatement.Insert[StateT] = {
    (key: Key, timestamp: Instant, head: Head, origin: Option[Origin]) => {
      StateT.unit { state =>
        val entry = HeadEntry(
          partitionOffset = head.partitionOffset,
          segmentSize = head.segmentSize,
          seqNr = head.seqNr,
          deleteTo = head.deleteTo,
          created = timestamp,
          updated = timestamp,
          origin = origin)
        val entries = state
          .head
          .getOrElse(key.topic, Map.empty)
          .updated(key.id, entry)
        state.copy(head = state.head.updated(key.topic, entries))
      }
    }
  }


  val selectHead: HeadStatement.Select[StateT] = {
    key: Key => {
      StateT.success { state =>
        val head = for {
          entries <- state.head.get(key.topic)
          entry   <- entries.get(key.id)
        } yield {
          Head(
            partitionOffset = entry.partitionOffset,
            segmentSize = entry.segmentSize,
            seqNr = entry.seqNr,
            deleteTo = entry.deleteTo)
        }
        (state, head)
      }
    }
  }


  val updateHead: HeadStatement.Update[StateT] = {
    (key: Key, partitionOffset: PartitionOffset, timestamp: Instant, seqNr: SeqNr, deleteTo: SeqNr) => {
      StateT.unit { state =>
        state.updateHead(key) { entry =>
          entry.copy(
            partitionOffset = partitionOffset,
            updated = timestamp,
            seqNr = seqNr,
            deleteTo = deleteTo.some)
        }
      }
    }
  }


  val updateHeadSeqNr: HeadStatement.UpdateSeqNr[StateT] = {
    (key: Key, partitionOffset: PartitionOffset, timestamp: Instant, seqNr: SeqNr) => {
      StateT.unit { state =>
        state.updateHead(key) { entry =>
          entry.copy(
            partitionOffset = partitionOffset,
            updated = timestamp,
            seqNr = seqNr)
        }
      }
    }
  }


  val updateHeadDeleteTo: HeadStatement.UpdateDeleteTo[StateT] = {
    (key: Key, partitionOffset: PartitionOffset, timestamp: Instant, deleteTo: SeqNr) => {
      StateT.unit { state =>
        state.updateHead(key) { entry =>
          entry.copy(
            partitionOffset = partitionOffset,
            updated = timestamp,
            deleteTo = deleteTo.some)
        }
      }
    }
  }


  val selectPointer: PointerStatement.Select[StateT] = {
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


  val selectPointersIn: PointerStatement.SelectIn[StateT] = {
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


  val selectPointers: PointerStatement.SelectAll[StateT] = {
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


  val insertPointer: PointerStatement.Insert[StateT] = {
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


  val updatePointer: PointerStatement.Update[StateT] = {
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


  val selectTopics: PointerStatement.SelectTopics[StateT] = {
    () => {
      StateT.success { state =>
        val topics = state.pointers.keys.toList
        (state, topics)
      }
    }
  }


  val statements: ReplicatedCassandra.Statements[StateT] = ReplicatedCassandra.Statements(
    insertRecords,
    deleteRecords,
    insertHead,
    selectHead,
    updateHead,
    updateHeadSeqNr,
    updateHeadDeleteTo,
    selectPointer,
    selectPointersIn,
    selectPointers,
    insertPointer,
    updatePointer,
    selectTopics)


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

  final case class HeadEntry(
    partitionOffset: PartitionOffset,
    segmentSize: Int,
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
    head: Map[Topic, Map[String, HeadEntry]] = Map.empty,
    journal: Map[(Key, SegmentNr), Map[(SeqNr, Instant), EventRecord]] = Map.empty)

  object State {

    val empty: State = State()
    

    implicit class StateOps(val self: State) extends AnyVal {

      def updateHead(key: Key)(f: HeadEntry => HeadEntry): State = {
        val state = for {
          entries <- self.head.get(key.topic)
          entry   <- entries.get(key.id)
        } yield {
          val entry1 = f(entry)
          val entries1 = entries.updated(key.id, entry1)
          self.copy(head = self.head.updated(key.topic, entries1))
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
