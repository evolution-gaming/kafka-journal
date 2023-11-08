package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.effect.IO
import cats.effect.kernel.{Concurrent, Ref}
import cats.syntax.all._
import com.evolutiongaming.kafka.journal.IOSuite._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.EventualPayloadAndType
import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant

class SnapshotCassandraTest extends AnyFunSuite {

  type SnaphsotWithPayload = SnapshotRecord[EventualPayloadAndType]

  test("save and load") {
    val program = for {
      statements <- statements[IO]
      store = SnapshotCassandra(statements)
      key = Key("topic", "id")
      _ <- store.save(key, snapshot)
      snapshot <- store.load(key, SeqNr.min, Instant.MAX, SeqNr.max, Instant.MAX)
    } yield {
      assert(snapshot.isDefined)
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

  val snapshot = SnapshotRecord(
    snapshot = Snapshot(
      seqNr = SeqNr.min,
      payload = Some(EventualPayloadAndType(payload = Left("payload"), payloadType = PayloadType.Text))
    ),
    timestamp = Instant.MIN,
    origin = None,
    version = None
  )

}
