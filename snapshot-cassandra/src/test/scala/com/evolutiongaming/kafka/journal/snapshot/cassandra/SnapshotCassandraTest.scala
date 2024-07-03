package com.evolutiongaming.kafka.journal.snapshot.cassandra

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

  val numberOfSnapshots: Int = 10

  test("save and load") {
    val program = for {
      statements <- statements.pure[F]
      store = SnapshotCassandra(statements, numberOfSnapshots)
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
      store = SnapshotCassandra[F](
        statements.copy(selectMetadata = { _ =>
          // sync data after first call to simulate delayed update
          // otherwise the `selectMetadata` call may be stuck in an infinite loop
          DatabaseState.metadata <* DatabaseState.sync
        }),
        numberOfSnapshots
      )
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
      store = SnapshotCassandra[F](statements, numberOfSnapshots)
      key = Key("topic", "id")
      // try to save twice
      _ <- store.save(key, record)
      _ <- DatabaseState.sync
      _ <- store.save(key, record)
      _ <- DatabaseState.sync
      size <- DatabaseState.size
    } yield {
      // we should only get one snapshot in a database
      assert(size == 1)
    }
    program.run(DatabaseState.empty).get
  }

  test("drop all") {
    val program = for {
      statements <- statements.pure[F]
      store = SnapshotCassandra(statements, numberOfSnapshots)
      key = Key("topic", "id")
      snapshot1 = record.snapshot.copy(seqNr = SeqNr.unsafe(1))
      snapshot2 = record.snapshot.copy(seqNr = SeqNr.unsafe(2))
      _ <- store.save(key, record.copy(snapshot = snapshot1))
      _ <- DatabaseState.sync
      _ <- store.save(key, record.copy(snapshot = snapshot2))
      _ <- DatabaseState.sync
      _ <- store.delete(key, SnapshotSelectionCriteria.All)
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
      store = SnapshotCassandra(statements, numberOfSnapshots)
      key = Key("topic", "id")
      snapshot1 = record.snapshot.copy(seqNr = SeqNr.unsafe(1))
      snapshot2 = record.snapshot.copy(seqNr = SeqNr.unsafe(2))
      _ <- store.save(key, record.copy(snapshot = snapshot1))
      _ <- DatabaseState.sync
      _ <- store.save(key, record.copy(snapshot = snapshot2))
      _ <- DatabaseState.sync
      _ <- store.delete(key, SeqNr.unsafe(2))
      _ <- DatabaseState.sync
      snapshot <- store.load(key, SnapshotSelectionCriteria.All)
    } yield {
      assert(snapshot.map(_.snapshot.seqNr) == SeqNr.opt(1), "(snapshot1 should still be in a database)")
    }
    program.run(DatabaseState.empty).get
  }

  def statements: SnapshotCassandra.Statements[F] = SnapshotCassandra.Statements(
    insertRecord = { (_, bufferNr, snapshot) =>
      DatabaseState.insert(bufferNr, snapshot)
    },
    updateRecord = { (_, bufferNr, insertSnapshot, deleteSnapshot) =>
      DatabaseState.update(bufferNr, insertSnapshot, deleteSnapshot)
    },
    selectRecords = { (_, bufferNr) =>
      DatabaseState.select(bufferNr)
    },
    selectMetadata = { _ =>
      DatabaseState.metadata
    },
    deleteRecords = { (_, bufferNr) =>
      DatabaseState.delete(bufferNr)
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

    def delete(bufferNr: BufferNr): Option[DatabaseState] =
      Option.when(stored.contains(bufferNr)) {
        this.copy(stored = stored - bufferNr)
      }

    def select(bufferNr: BufferNr): Option[SnaphsotWithPayload] =
      availableForReading.get(bufferNr)

    def metadata: Map[BufferNr, (SeqNr, Instant)] =
      availableForReading.fmap { snapshot =>
        (snapshot.snapshot.seqNr, snapshot.timestamp)
      }

    def size: Int = stored.size

    def sync: DatabaseState =
      this.copy(availableForReading = stored)

  }
  object DatabaseState {

    def empty: DatabaseState =
      DatabaseState(stored = Map.empty, availableForReading = Map.empty)

    def insert(bufferNr: BufferNr, snapshot: SnaphsotWithPayload): F[Boolean] =
      DatabaseState.tryModify(_.insert(bufferNr, snapshot))

    def update(bufferNr: BufferNr, insertSnapshot: SnaphsotWithPayload, deleteSnapshot: SeqNr): F[Boolean] =
      DatabaseState.tryModify(_.update(bufferNr, insertSnapshot, deleteSnapshot))

    def delete(bufferNr: BufferNr): F[Boolean] =
      DatabaseState.tryModify(_.delete(bufferNr))

    def select(bufferNr: BufferNr): F[Option[SnaphsotWithPayload]] =
      DatabaseState.get.map(_.select(bufferNr))

    def metadata: F[Map[BufferNr, (SeqNr, Instant)]] =
      DatabaseState.get.map(_.metadata)

    def size: F[Int] = DatabaseState.get.map(_.size)

    def sync: F[Unit] =
      StateT.modify(_.sync)

    private def set(state: DatabaseState): F[Unit] =
      StateT.set(state)

    private def get: F[DatabaseState] =
      StateT.get

    private def tryModify(f: DatabaseState => Option[DatabaseState]): F[Boolean] =
      for {
        state0 <- DatabaseState.get
        state1 = f(state0)
        _ <- state1.traverse(DatabaseState.set)
        wasApplied = state1.isDefined
      } yield wasApplied

  }

  val record = SnapshotRecord(
    snapshot = Snapshot(
      seqNr = SeqNr.min,
      payload = EventualPayloadAndType(payload = Left("payload"), payloadType = PayloadType.Text)
    ),
    timestamp = Instant.MIN,
    origin = None,
    version = None
  )

}
