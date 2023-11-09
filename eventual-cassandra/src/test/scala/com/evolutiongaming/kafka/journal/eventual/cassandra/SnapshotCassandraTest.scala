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
  type F[A] = cats.data.StateT[Try, DatabaseState, A]

  test("save and load") {
    val program = for {
      statements <- statements.pure[F]
      store = SnapshotCassandra(statements)
      key = Key("topic", "id")
      _ <- store.save(key, record)
      snapshot <- store.load(key, SeqNr.max, Instant.MAX, SeqNr.min, Instant.MIN)
    } yield {
      assert(snapshot.isDefined)
    }
    program.run(DatabaseState.empty).get
  }

  test("save concurrently") {
    val program = for {
      statements <- statements.pure[F]
      // both snapshotters see empty metadata, because it is not saved yet
      emptyStore = SnapshotCassandra[F](statements.copy(selectMetadata = { (_, _) => Map.empty.pure[F] }))
      finalStore = SnapshotCassandra[F](statements)
      key = Key("topic", "id")
      snapshot1 = record.snapshot.copy(seqNr = SeqNr.unsafe(1))
      snapshot2 = record.snapshot.copy(seqNr = SeqNr.unsafe(2))
      // here, we change the order of writing, to simulate concurrency
      _ <- emptyStore.save(key, record.copy(snapshot = snapshot2))
      _ <- emptyStore.save(key, record.copy(snapshot = snapshot1))
      // we should still get the latest snapshot here
      snapshot <- finalStore.load(key, SeqNr.max, Instant.MAX, SeqNr.min, Instant.MIN)
    } yield {
      assert(snapshot.map(_.snapshot.seqNr) == Some(SeqNr.unsafe(2)))
    }
    program.run(DatabaseState.empty).get
  }

  def statements: SnapshotCassandra.Statements[F] = SnapshotCassandra.Statements(
    insertRecords = { (_, _, bufferNr, snapshot) =>
      StateT.modify(_.insert(bufferNr, snapshot))
    },
    selectRecords = { (_, _, bufferNr) =>
      StateT.get[Try, DatabaseState].map(_.select(bufferNr))
    },
    selectMetadata = { (_, _) =>
      StateT.get[Try, DatabaseState].map(_.metadata)
    },
    deleteRecords = { (_, _, bufferNr) =>
      StateT.modify(_.delete(bufferNr))
    }
  )

  case class DatabaseState(snapshots: Map[BufferNr, SnaphsotWithPayload]) {
    def insert(bufferNr: BufferNr, snapshot: SnaphsotWithPayload): DatabaseState =
      this.copy(snapshots = snapshots.updated(bufferNr, snapshot))
    def select(bufferNr: BufferNr): Option[SnaphsotWithPayload] =
      snapshots.get(bufferNr)
    def metadata: Map[BufferNr, (SeqNr, Instant)] =
      snapshots.fmap { snapshot =>
        (snapshot.snapshot.seqNr, snapshot.timestamp)
      }
    def delete(bufferNr: BufferNr): DatabaseState =
      this.copy(snapshots = snapshots - bufferNr)
  }
  object DatabaseState {
    def empty: DatabaseState = DatabaseState(Map.empty)
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
