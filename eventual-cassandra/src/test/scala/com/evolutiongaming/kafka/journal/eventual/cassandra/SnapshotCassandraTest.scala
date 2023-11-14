package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.data.StateT
import cats.syntax.all._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.EventualPayloadAndType
import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant
import scala.util.Try

class SnapshotCassandraTest extends AnyFunSuite {

  type SnaphsotWithPayload = SnapshotRecord[EventualPayloadAndType]
  type F[A] = StateT[Try, DatabaseState, A]

  test("save and load") {
    val program = for {
      statements <- statements.pure[F]
      store = SnapshotCassandra(statements)
      key = Key("topic", "id")
      _ <- store.save(key, record)
      _ <- DatabaseState.sync
      snapshot <- store.load(key, SnapshotSelectionCriteria.All)
    } yield {
      assert(snapshot.isDefined, "(could not load the saved snapshot)")
    }
    program.run(DatabaseState.empty).get
  }

  test("save concurrently") {
    val program = for {
      statements <- statements.pure[F]
      // both snapshotters see empty metadata, because it is not saved yet
      store = SnapshotCassandra[F](statements.copy(selectMetadata = { (_, _) =>
        // sync data after first call to simulate delayed update
        // otherwise the `selectMetadata` call may be stuck in an infinite loop
        DatabaseState.get.map(_.metadata) <* DatabaseState.sync
      }))
      key = Key("topic", "id")
      snapshot1 = record.snapshot.copy(seqNr = SeqNr.unsafe(1))
      snapshot2 = record.snapshot.copy(seqNr = SeqNr.unsafe(2))
      // here, we change the order of writing, to simulate concurrency
      _ <- store.save(key, record.copy(snapshot = snapshot2))
      _ <- store.save(key, record.copy(snapshot = snapshot1))
      _ <- DatabaseState.sync
      // we should still get the latest snapshot here
      snapshot <- store.load(key, SnapshotSelectionCriteria.All)
    } yield {
      assert(snapshot.map(_.snapshot.seqNr) == SeqNr.opt(2), "(last snapshot is not seen)")
    }
    program.run(DatabaseState.empty).get
  }

  test("save is idempotent") {
    val program = for {
      statements <- statements.pure[F]
      store = SnapshotCassandra[F](statements)
      key = Key("topic", "id")
      // try to save twice
      _ <- store.save(key, record)
      _ <- DatabaseState.sync
      _ <- store.save(key, record)
      _ <- DatabaseState.sync
      state <- DatabaseState.get
    } yield {
      // we should only get one snapshot in a database
      assert(state.stored.size == 1)
    }
    program.run(DatabaseState.empty).get
  }

  test("drop all") {
    val program = for {
      statements <- statements.pure[F]
      store = SnapshotCassandra(statements)
      key = Key("topic", "id")
      snapshot1 = record.snapshot.copy(seqNr = SeqNr.unsafe(1))
      snapshot2 = record.snapshot.copy(seqNr = SeqNr.unsafe(2))
      _ <- store.save(key, record.copy(snapshot = snapshot1))
      _ <- DatabaseState.sync
      _ <- store.save(key, record.copy(snapshot = snapshot2))
      _ <- DatabaseState.sync
      _ <- store.drop(key, SnapshotSelectionCriteria.All)
      _ <- DatabaseState.sync
      snapshot <- store.load(key, SnapshotSelectionCriteria.All)
    } yield {
      assert(snapshot.isEmpty, "(some snapshots were not dropped)")
    }
    program.run(DatabaseState.empty).get
  }

  test("drop by seqNr") {
    val program = for {
      statements <- statements.pure[F]
      store = SnapshotCassandra(statements)
      key = Key("topic", "id")
      snapshot1 = record.snapshot.copy(seqNr = SeqNr.unsafe(1))
      snapshot2 = record.snapshot.copy(seqNr = SeqNr.unsafe(2))
      _ <- store.save(key, record.copy(snapshot = snapshot1))
      _ <- DatabaseState.sync
      _ <- store.save(key, record.copy(snapshot = snapshot2))
      _ <- DatabaseState.sync
      _ <- store.drop(key, SeqNr.unsafe(2))
      _ <- DatabaseState.sync
      snapshot <- store.load(key, SnapshotSelectionCriteria.All)
    } yield {
      assert(snapshot.map(_.snapshot.seqNr) == SeqNr.opt(1), "(snapshot1 should still be in a database)")
    }
    program.run(DatabaseState.empty).get
  }

  def statements: SnapshotCassandra.Statements[F] = SnapshotCassandra.Statements(
    insertRecord = { (_, _, bufferNr, snapshot) =>
      for {
        state0 <- DatabaseState.get
        state1 = state0.insert(bufferNr, snapshot)
        wasApplied = state1.isDefined
        _ <- DatabaseState.set(state1.getOrElse(state0))
      } yield wasApplied
    },
    updateRecord = { (_, _, bufferNr, insertSnapshot, deleteSnapshot) =>
      for {
        state0 <- DatabaseState.get
        state1 = state0.update(bufferNr, insertSnapshot, deleteSnapshot)
        wasApplied = state1.isDefined
        _ <- DatabaseState.set(state1.getOrElse(state0))
      } yield wasApplied
    },
    selectRecords = { (_, _, bufferNr) =>
      DatabaseState.get.map(_.select(bufferNr))
    },
    selectMetadata = { (_, _) =>
      DatabaseState.get.map(_.metadata)
    },
    deleteRecords = { (_, _, bufferNr) =>
      DatabaseState.modify(_.delete(bufferNr))
    }
  )

  case class DatabaseState(
    stored: Map[BufferNr, SnaphsotWithPayload],
    availableForReading: Map[BufferNr, SnaphsotWithPayload]
  ) {

    def insert(bufferNr: BufferNr, snapshot: SnaphsotWithPayload): Option[DatabaseState] =
      Option.when(!stored.contains(bufferNr)) {
        this.copy(stored = stored.updated(bufferNr, snapshot))
      }

    def update(bufferNr: BufferNr, insertSnapshot: SnaphsotWithPayload, deleteSnapshot: SeqNr): Option[DatabaseState] =
      stored.get(bufferNr).flatMap { previousSnapshot =>
        Option.when(previousSnapshot.snapshot.seqNr == deleteSnapshot) {
          this.copy(stored = stored.updated(bufferNr, insertSnapshot))
        }
      }

    def select(bufferNr: BufferNr): Option[SnaphsotWithPayload] =
      availableForReading.get(bufferNr)

    def metadata: Map[BufferNr, (SeqNr, Instant)] =
      availableForReading.fmap { snapshot =>
        (snapshot.snapshot.seqNr, snapshot.timestamp)
      }

    def delete(bufferNr: BufferNr): DatabaseState =
      this.copy(stored = stored - bufferNr)

    def sync: DatabaseState =
      this.copy(availableForReading = stored)

  }
  object DatabaseState {

    def empty: DatabaseState =
      DatabaseState(stored = Map.empty, availableForReading = Map.empty)

    def set(state: DatabaseState): StateT[Try, DatabaseState, Unit] =
      StateT.set(state)

    def get: StateT[Try, DatabaseState, DatabaseState] =
      StateT.get

    def modify(f: DatabaseState => DatabaseState): StateT[Try, DatabaseState, Unit] =
      StateT.modify(f)

    def sync: StateT[Try, DatabaseState, Unit] =
      StateT.modify(_.sync)

  }

  val record = SnapshotRecord(
    snapshot = Snapshot(
      seqNr = SeqNr.min,
      payload = Some(EventualPayloadAndType(payload = Left("payload"), payloadType = PayloadType.Text))
    ),
    timestamp = Instant.MIN,
    origin = None,
    version = None
  )

}
