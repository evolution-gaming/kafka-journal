package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.effect.IO
import cats.effect.kernel.{Concurrent, Ref}
import cats.syntax.all._
import com.evolutiongaming.kafka.journal.IOSuite._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.EventualPayloadAndType
import org.scalatest.funsuite.AsyncFunSuite

import java.time.Instant

class SnapshotCassandraTest extends AsyncFunSuite {

  type SnaphsotWithPayload = SnapshotRecord[EventualPayloadAndType]

  test("save and load") {
    val program = for {
      statements <- statements[IO]
      store = SnapshotCassandra(statements)
      key = Key("topic", "id")
      _ <- store.save(key, record)
      snapshot <- store.load(key, SeqNr.max, Instant.MAX, SeqNr.min, Instant.MIN)
    } yield {
      assert(snapshot.isDefined)
    }
    program.run()
  }

  test("save concurrently") {
    val program = for {
      statements <- statements[IO]
      store = SnapshotCassandra(statements)
      key = Key("topic", "id")
      snapshot1 = record.snapshot.copy(seqNr = SeqNr.unsafe(1))
      snapshot2 = record.snapshot.copy(seqNr = SeqNr.unsafe(2))
      save1 = store.save(key, record.copy(snapshot = snapshot1))
      save2 = store.save(key, record.copy(snapshot = snapshot2))
      _ <- IO.both(save1, save2)
      snapshot <- store.load(key, SeqNr.max, Instant.MAX, SeqNr.min, Instant.MIN)
    } yield {
      assert(snapshot.map(_.snapshot.seqNr) == Some(SeqNr.unsafe(2)))
    }
    program.run()
  }

  def statements[F[_]: Concurrent]: F[SnapshotCassandra.Statements[F]] =
    for {
      state <- Ref[F].of(DatabaseState.empty)
    } yield SnapshotCassandra.Statements(
      insertRecords = { (_, _, bufferNr, snapshot) =>
        state.update(_.insert(bufferNr, snapshot))
      },
      selectRecords = { (_, _, bufferNr) =>
        state.get.map(_.select(bufferNr))
      },
      selectMetadata = { (_, _) =>
        state.get.map(_.metadata)
      },
      deleteRecords = { (_, _, bufferNr) =>
        state.update(_.delete(bufferNr))
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
