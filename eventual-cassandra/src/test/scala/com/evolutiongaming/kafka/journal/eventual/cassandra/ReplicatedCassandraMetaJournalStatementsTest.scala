package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.{Instant, LocalDate}

import cats.syntax.all._
import com.evolutiongaming.kafka.journal.ExpireAfter.implicits._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.cassandra.ExpireOn.implicits._
import com.evolutiongaming.kafka.journal.eventual.cassandra.ReplicatedCassandra.MetaJournalStatements.ByKey
import com.evolutiongaming.kafka.journal.util.TemporalHelper._
import com.evolutiongaming.skafka.Topic
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._
import scala.util.Try
import org.scalatest.wordspec.AnyWordSpec

class ReplicatedCassandraMetaJournalStatementsTest extends AnyWordSpec with Matchers {
  import ReplicatedCassandraMetaJournalStatementsTest._

  "byKey" when {

    "journalHead" should {

      "empty" in {
        val stateT = for {
          a <- byKey.journalHead
          _  = a shouldEqual none
        } yield {}
        val (state, _) = stateT.run(State.empty).get
        state shouldEqual State.empty
      }

      "from metadata" in {
        val key = ReplicatedCassandraMetaJournalStatementsTest.key
        val journalHead1 = journalHead.copy(deleteTo = journalHead.seqNr.toDeleteTo.some)
        val stateT = for {
          a <- byKey.journalHead
          _  = a shouldEqual journalHead1.some
        } yield {}
        val state0 = State(
          metadata = Map((key.topic, Map((key.id, MetaJournalEntry(
            journalHead = journalHead1,
            created = timestamp0,
            updated = timestamp1,
            origin = origin.some))))))
        val (state1, _) = stateT.run(state0).get
        val expected = State(
          metaJournal = Map(((key.topic, segment), Map((key.id, MetaJournalEntry(
            journalHead = journalHead1,
            created = timestamp0,
            updated = timestamp1,
            origin = origin.some))))))
        state1 shouldEqual expected
      }

      "from metaJournal" in {
        val key = ReplicatedCassandraMetaJournalStatementsTest.key
        val journalHead1 = journalHead.copy(deleteTo = journalHead.seqNr.toDeleteTo.some)
        val stateT = for {
          a <- byKey.journalHead
          _  = a shouldEqual journalHead1.some
        } yield {}
        val expected = State(
          metaJournal = Map(((key.topic, segment), Map((key.id, MetaJournalEntry(
            journalHead = journalHead1,
            created = timestamp0,
            updated = timestamp1,
            origin = origin.some))))))
        val (state1, _) = stateT.run(expected).get
        state1 shouldEqual expected
      }
    }

    "insert" in {
      val key = ReplicatedCassandraMetaJournalStatementsTest.key
      val stateT = for {
        a <- byKey.insert(timestamp0, journalHead, origin.some)
        _  = a.shouldEqual(())
      } yield {}
      val expected = State(
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
      val (state1, _) = stateT.run(expected).get
      state1 shouldEqual expected
    }

    "update" should {

      val update = byKey.update(partitionOffset, timestamp1)

      "update" in {
        val key = ReplicatedCassandraMetaJournalStatementsTest.key
        val stateT = for {
          _ <- byKey.insert(timestamp0, journalHead, origin.some)
          a <- update(SeqNr.max, SeqNr.max.toDeleteTo)
          _  = a.shouldEqual(())
        } yield {}
        val journalHead1 = journalHead.copy(seqNr = SeqNr.max, deleteTo = SeqNr.max.toDeleteTo.some)
        val expected = State(
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
        val (state1, _) = stateT.run(expected).get
        state1 shouldEqual expected
      }

      "seqNr" in {
        val key = ReplicatedCassandraMetaJournalStatementsTest.key
        val stateT = for {
          _ <- byKey.insert(timestamp0, journalHead, origin.some)
          a <- update(SeqNr.max)
          _  = a.shouldEqual(())
        } yield {}
        val journalHead1 = journalHead.copy(seqNr = SeqNr.max)
        val expected = State(
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
        val (state1, _) = stateT.run(expected).get
        state1 shouldEqual expected
      }

      "expiry" in {
        val key = ReplicatedCassandraMetaJournalStatementsTest.key
        val stateT = for {
          _ <- byKey.insert(timestamp0, journalHead, origin.some)
          a <- update(SeqNr.max, expiry)
          _  = a.shouldEqual(())
        } yield {}
        val expected = State(
          metadata = Map((key.topic, Map((key.id, MetaJournalEntry(
            journalHead = journalHead.copy(seqNr = SeqNr.max),
            created = timestamp0,
            updated = timestamp1,
            origin = origin.some))))),
          metaJournal = Map(((key.topic, segment), Map((key.id, MetaJournalEntry(
            journalHead = journalHead.copy(seqNr = SeqNr.max, expiry = expiry.some),
            created = timestamp0,
            updated = timestamp1,
            origin = origin.some))))))
        val (state1, _) = stateT.run(expected).get
        state1 shouldEqual expected
      }

      "deleteTo" in {
        val key = ReplicatedCassandraMetaJournalStatementsTest.key
        val stateT = for {
          _ <- byKey.insert(timestamp0, journalHead, origin.some)
          a <- update(journalHead.seqNr.toDeleteTo)
          _  = a.shouldEqual(())
        } yield {}
        val journalHead1 = journalHead.copy(deleteTo = journalHead.seqNr.toDeleteTo.some)
        val expected = State(
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
        val (state1, _) = stateT.run(expected).get
        state1 shouldEqual expected
      }
    }

    "delete" in {
      val stateT = for {
        _ <- byKey.insert(timestamp0, journalHead, origin.some)
        a <- byKey.delete
        _  = a.shouldEqual(())
      } yield {}
      val (state, _) = stateT.run(State.empty).get
      state shouldEqual State.empty
    }

    "deleteExpiry" in {
      val key = ReplicatedCassandraMetaJournalStatementsTest.key
      val stateT = for {
        _ <- byKey.insert(timestamp0, journalHead, origin.some)
        _ <- byKey.update(partitionOffset, timestamp1)(SeqNr.min, expiry)
        a <- byKey.deleteExpiry
        _  = a.shouldEqual(())
      } yield {}
      val (state, _) = stateT.run(State.empty).get
      val expected = State(
        metaJournal = Map(((key.topic, segment), Map((key.id, MetaJournalEntry(
          journalHead = journalHead,
          created = timestamp0,
          updated = timestamp1,
          origin = origin.some))))))
      state shouldEqual expected
    }
  }
}

object ReplicatedCassandraMetaJournalStatementsTest {

  val timestamp0: Instant = Instant.now()
  val timestamp1: Instant = timestamp0 + 1.second
  val key: Key = Key(id = "id", topic = "topic")
  val segment: SegmentNr = SegmentNr.min
  val partitionOffset: PartitionOffset = PartitionOffset.empty
  val journalHead: JournalHead = JournalHead(partitionOffset, SegmentSize.default, SeqNr.min)
  val origin: Origin = Origin("origin")
  val expiry: Expiry = Expiry(
    1.day.toExpireAfter,
    LocalDate.of(2019, 12, 12).toExpireOn)


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
    (key, segment, created, updated, journalHead, origin) => {
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


  val selectMetaJournal: MetaJournalStatements.SelectJournalHead[StateT] = {
    (key, segment) => {
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
    (key, segment, partitionOffset, timestamp, seqNr, deleteTo) => {
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


  val updateSeqNrMetaJournal: MetaJournalStatements.UpdateSeqNr[StateT] = {
    (key, segment, partitionOffset, timestamp, seqNr) => {
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


  val updateExpiryMetaJournal: MetaJournalStatements.UpdateExpiry[StateT] = {
    (key, segment, partitionOffset, timestamp, seqNr, expiry) => {
      StateT.unit { state =>
        state.updateMetaJournal(key, segment) { entry =>
          entry.copy(
            journalHead = entry.journalHead.copy(
              partitionOffset = partitionOffset,
              seqNr = seqNr,
              expiry = expiry.some),
            updated = timestamp)
        }
      }
    }
  }


  val updateDeleteToMetaJournal: MetaJournalStatements.UpdateDeleteTo[StateT] = {
    (key, segment, partitionOffset, timestamp, deleteTo) => {
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
    (key, segment) => {
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


  val deleteExpiryMetaJournal: MetaJournalStatements.DeleteExpiry[StateT] = {
    (key, segment) => {
      StateT.unit { state =>
        state.updateMetaJournal(key, segment) { entry =>
          entry.copy(
            journalHead = entry.journalHead.copy(
              partitionOffset = partitionOffset,
              expiry = none))
        }
      }
    }
  }


  val selectJournalHeadMetadata: MetadataStatements.SelectJournalHead[StateT] = {
    key => {
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
    key => {
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


  val deleteMetadata: MetadataStatements.Delete[StateT] = {
    key => {
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
    selectMetaJournal,
    insertMetaJournal,
    updateMetaJournal,
    updateSeqNrMetaJournal,
    updateExpiryMetaJournal,
    updateDeleteToMetaJournal,
    deleteMetaJournal,
    deleteExpiryMetaJournal)


  val metaJournalStatements: ReplicatedCassandra.MetaJournalStatements[StateT] = {
    ReplicatedCassandra.MetaJournalStatements(
      metaJournal = metaJournal,
      selectMetadata = selectMetadata,
      deleteMetadata = deleteMetadata,
      insertMetaJournal = insertMetaJournal)
  }

  val byKey: ByKey[StateT] = metaJournalStatements(key, segment)
}
