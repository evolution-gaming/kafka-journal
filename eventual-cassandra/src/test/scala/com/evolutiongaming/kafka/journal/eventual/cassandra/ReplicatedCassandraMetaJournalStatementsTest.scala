package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.Instant

import cats.implicits._
import com.evolutiongaming.kafka.journal.util.TemporalHelper._
import com.evolutiongaming.kafka.journal.{Key, Origin, PartitionOffset, SeqNr}
import com.evolutiongaming.skafka.Topic
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._
import scala.util.Try

class ReplicatedCassandraMetaJournalStatementsTest extends AnyFunSuite with Matchers {
  import ReplicatedCassandraMetaJournalStatementsTest._

  test("journalHead empty") {
    val key = ReplicatedCassandraMetaJournalStatementsTest.key
    val stateT = for {
      a <- metaJournalStatements.journalHead(key, segment)
      _  = a shouldEqual none
    } yield {}
    val (state, _) = stateT.run(State.empty).get
    state shouldEqual State.empty
  }

  test("journalHead from metadata") {
    val key = ReplicatedCassandraMetaJournalStatementsTest.key
    val journalHead1 = journalHead.copy(deleteTo = journalHead.seqNr.some)
    val stateT = for {
      a <- metaJournalStatements.journalHead(key, segment)
      _  = a shouldEqual journalHead1.some
    } yield {}
    val state0 = State(
      metadata = Map((key.topic, Map((key.id, MetaJournalEntry(
        journalHead = journalHead1,
        created = timestamp0,
        updated = timestamp1,
        origin = origin.some))))))
    val (state1, _) = stateT.run(state0).get
    state1 shouldEqual State(
      metadata = Map((key.topic, Map((key.id, MetaJournalEntry(
        journalHead = journalHead1,
        created = timestamp0,
        updated = timestamp1,
        origin = origin.some))))),
      metaJournal = Map(((key.topic, segment), Map((key.id, MetaJournalEntry(
        journalHead = journalHead1,
        created = timestamp0,
        updated = timestamp1,
        origin = origin.some))))))
  }

  test("journalHead from metaJournal") {
    val key = ReplicatedCassandraMetaJournalStatementsTest.key
    val journalHead1 = journalHead.copy(deleteTo = journalHead.seqNr.some)
    val stateT = for {
      a <- metaJournalStatements.journalHead(key, segment)
      _  = a shouldEqual journalHead1.some
    } yield {}
    val state0 = State(
      metaJournal = Map(((key.topic, segment), Map((key.id, MetaJournalEntry(
        journalHead = journalHead1,
        created = timestamp0,
        updated = timestamp1,
        origin = origin.some))))))
    val (state1, _) = stateT.run(state0).get
    state1 shouldEqual state0
  }

  test("insert") {
    val key = ReplicatedCassandraMetaJournalStatementsTest.key
    val stateT = for {
      a <- metaJournalStatements.insert(key, segment, timestamp0, journalHead, origin.some)
      _  = a.shouldEqual(())
    } yield {}
    val state0 = State(
      metadata = Map((key.topic, Map((key.id, MetaJournalEntry(
        journalHead = journalHead,
        created = timestamp0,
        updated = timestamp0,
        origin = origin.some))))),
      metaJournal = Map(((key.topic, segment), Map((key.id, MetaJournalEntry(
        journalHead = journalHead,
        created = timestamp0,
        updated = timestamp0,
        origin = origin.some))))))
    val (state1, _) = stateT.run(state0).get
    state1 shouldEqual state0
  }

  test("update") {
    val key = ReplicatedCassandraMetaJournalStatementsTest.key
    val stateT = for {
      a <- metaJournalStatements.insert(key, segment, timestamp0, journalHead, origin.some)
      _  = a.shouldEqual(())
      a <- metaJournalStatements.update(key, segment, partitionOffset, timestamp1, SeqNr.max, SeqNr.max)
      _  = a.shouldEqual(())
    } yield {}
    val journalHead1 = journalHead.copy(seqNr = SeqNr.max, deleteTo = SeqNr.max.some)
    val state0 = State(
      metadata = Map((key.topic, Map((key.id, MetaJournalEntry(
        journalHead = journalHead1,
        created = timestamp0,
        updated = timestamp1,
        origin = origin.some))))),
      metaJournal = Map(((key.topic, segment), Map((key.id, MetaJournalEntry(
        journalHead = journalHead1,
        created = timestamp0,
        updated = timestamp1,
        origin = origin.some))))))
    val (state1, _) = stateT.run(state0).get
    state1 shouldEqual state0
  }

  test("updateSeqNr") {
    val key = ReplicatedCassandraMetaJournalStatementsTest.key
    val stateT = for {
      a <- metaJournalStatements.insert(key, segment, timestamp0, journalHead, origin.some)
      _  = a.shouldEqual(())
      a <- metaJournalStatements.updateSeqNr(key, segment, partitionOffset, timestamp1, SeqNr.max)
      _  = a.shouldEqual(())
    } yield {}
    val journalHead1 = journalHead.copy(seqNr = SeqNr.max)
    val state0 = State(
      metadata = Map((key.topic, Map((key.id, MetaJournalEntry(
        journalHead = journalHead1,
        created = timestamp0,
        updated = timestamp1,
        origin = origin.some))))),
      metaJournal = Map(((key.topic, segment), Map((key.id, MetaJournalEntry(
        journalHead = journalHead1,
        created = timestamp0,
        updated = timestamp1,
        origin = origin.some))))))
    val (state1, _) = stateT.run(state0).get
    state1 shouldEqual state0
  }

  test("updateDeleteTo") {
    val key = ReplicatedCassandraMetaJournalStatementsTest.key
    val stateT = for {
      a <- metaJournalStatements.insert(key, segment, timestamp0, journalHead, origin.some)
      _  = a.shouldEqual(())
      a <- metaJournalStatements.updateDeleteTo(key, segment, partitionOffset, timestamp1, journalHead.seqNr)
      _  = a.shouldEqual(())
    } yield {}
    val journalHead1 = journalHead.copy(deleteTo = journalHead.seqNr.some)
    val state0 = State(
      metadata = Map((key.topic, Map((key.id, MetaJournalEntry(
        journalHead = journalHead1,
        created = timestamp0,
        updated = timestamp1,
        origin = origin.some))))),
      metaJournal = Map(((key.topic, segment), Map((key.id, MetaJournalEntry(
        journalHead = journalHead1,
        created = timestamp0,
        updated = timestamp1,
        origin = origin.some))))))
    val (state1, _) = stateT.run(state0).get
    state1 shouldEqual state0
  }

  test("delete") {
    val key = ReplicatedCassandraMetaJournalStatementsTest.key
    val stateT = for {
      a <- metaJournalStatements.insert(key, segment, timestamp0, journalHead, origin.some)
      _  = a.shouldEqual(())
      a <- metaJournalStatements.delete(key, segment)
      _  = a.shouldEqual(())
    } yield {}
    val (state, _) = stateT.run(State.empty).get
    state shouldEqual State.empty
  }
}

object ReplicatedCassandraMetaJournalStatementsTest {

  val timestamp0: Instant = Instant.now()
  val timestamp1: Instant = timestamp0 + 1.second
  val key: Key = Key(id = "id", topic = "topic")
  val segment: SegmentNr = SegmentNr.min
  val partitionOffset: PartitionOffset = PartitionOffset.empty
  val journalHead: JournalHead = JournalHead(partitionOffset, SegmentSize.default, SeqNr.min, none)
  val origin: Origin = Origin("origin")


  final case class State(
    metadata: Map[Topic, Map[String, MetaJournalEntry]] = Map.empty,
    metaJournal: Map[(Topic, SegmentNr), Map[String, MetaJournalEntry]] = Map.empty)

  object State {

    val empty: State = State()


    implicit class StateOps(val self: State) extends AnyVal {

      def updateMetadata(key: Key)(f: MetaJournalEntry => MetaJournalEntry): State = {
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
    }
  }


  type StateT[A] = cats.data.StateT[Try, State, A]

  object StateT {

    def apply[A](f: State => Try[(State, A)]): StateT[A] = cats.data.StateT[Try, State, A](f)

    def success[A](f: State => (State, A)): StateT[A] = apply { s => f(s).pure[Try] }

    def unit(f: State => State): StateT[Unit] = success[Unit] { a => (f(a), ()) }
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


  val deleteMetaJournal: MetaJournalStatements.Delete[StateT] = {
    (key: Key, segment: SegmentNr) => {
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
        state1 getOrElse state
      }
    }
  }


  val insertMetadata: MetadataStatements.Insert[StateT] = {
    (key: Key, timestamp: Instant, journalHead: JournalHead, origin: Option[Origin]) => {
      StateT.unit { state =>
        val entry = MetaJournalEntry(
          journalHead = journalHead,
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


  val selectMetadataJournalHead: MetadataStatements.SelectJournalHead[StateT] = {
    key: Key => {
      StateT.success { state =>
        val journalHead = for {
          entries <- state.metadata.get(key.topic)
          entry   <- entries.get(key.id)
        } yield {
          entry.journalHead
        }
        (state, journalHead)
      }
    }
  }


  val selectMetadata: MetadataStatements.Select[StateT] = {
    key: Key => {
      StateT.success { state =>
        val entry = for {
          entries <- state.metadata.get(key.topic)
          entry   <- entries.get(key.id)
        } yield {
          entry
        }
        (state, entry)
      }
    }
  }


  val updateMetadata: MetadataStatements.Update[StateT] = {
    (key: Key, partitionOffset: PartitionOffset, timestamp: Instant, seqNr: SeqNr, deleteTo: SeqNr) => {
      StateT.unit { state =>
        state.updateMetadata(key) { entry =>
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


  val updateMetadataSeqNr: MetadataStatements.UpdateSeqNr[StateT] = {
    (key: Key, partitionOffset: PartitionOffset, timestamp: Instant, seqNr: SeqNr) => {
      StateT.unit { state =>
        state.updateMetadata(key) { entry =>
          entry.copy(
            journalHead = entry.journalHead.copy(
              partitionOffset = partitionOffset,
              seqNr = seqNr),
            updated = timestamp)
        }
      }
    }
  }


  val updateMetadataDeleteTo: MetadataStatements.UpdateDeleteTo[StateT] = {
    (key: Key, partitionOffset: PartitionOffset, timestamp: Instant, deleteTo: SeqNr) => {
      StateT.unit { state =>
        state.updateMetadata(key) { entry =>
          entry.copy(
            journalHead = entry.journalHead.copy(
              partitionOffset = partitionOffset,
              deleteTo = deleteTo.some),
            updated = timestamp)
        }
      }
    }
  }


  val deleteMetadata: MetadataStatements.Delete[StateT] = {
    key: Key => {
      StateT.unit { state =>
        val state1 = for {
          entries <- state.metadata.get(key.topic)
          _       <- entries.get(key.id)
        } yield {
          val entries1 = entries - key.id
          val metadata = if (entries1.isEmpty) {
            state.metadata - key.topic
          } else {
            state.metadata.updated(key.topic, entries1)
          }
          state.copy(metadata = metadata)
        }
        state1 getOrElse state
      }
    }
  }


  val metaJournal: ReplicatedCassandra.MetaJournalStatements[StateT] = ReplicatedCassandra.MetaJournalStatements(
    selectMetaJournalJournalHead,
    insertMetaJournal,
    updateMetaJournal,
    updateMetaJournalSeqNr,
    updateMetaJournalDeleteTo,
    deleteMetaJournal)


  val metadata: ReplicatedCassandra.MetaJournalStatements[StateT] = ReplicatedCassandra.MetaJournalStatements(
    selectMetadataJournalHead,
    insertMetadata,
    updateMetadata,
    updateMetadataSeqNr,
    updateMetadataDeleteTo,
    deleteMetadata)
  

  val metaJournalStatements: ReplicatedCassandra.MetaJournalStatements[StateT] = {
    ReplicatedCassandra.MetaJournalStatements(
      metaJournal = metaJournal,
      metadata = metadata,
      selectMetadata = selectMetadata,
      insertMetaJournal = insertMetaJournal)
  }
}
