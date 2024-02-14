package akka.persistence.kafka.journal

import akka.persistence.{SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria}
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all._
import cats.{Monad, Parallel, ~>}
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.EventualPayloadAndType
import com.evolutiongaming.kafka.journal.snapshot.cassandra.{SnapshotCassandra, SnapshotCassandraConfig}
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.scassandra.CassandraClusterOf

import java.time.Instant

trait SnapshotStoreAdapter[F[_]] {

  def load(persistenceId: String, criteria: SnapshotSelectionCriteria): F[Option[SelectedSnapshot]]

  def save(metadata: SnapshotMetadata, snapshot: Any): F[Unit]

  def delete(metadata: SnapshotMetadata): F[Unit]

  def delete(persistenceId: String, criteria: SnapshotSelectionCriteria): F[Unit]

}

object SnapshotStoreAdapter {

  def of[F[_]: Async: Parallel: LogOf: Fail, A](
    toKey: ToKey[F],
    origin: Option[Origin],
    snapshotSerializer: SnapshotSerializer[F, A],
    snapshotReadWrite: SnapshotReadWrite[F, A],
    config: SnapshotCassandraConfig,
    cassandraClusterOf: CassandraClusterOf[F]
  ): Resource[F, SnapshotStoreAdapter[F]] = {

    def adapter(
      store: SnapshotStoreFlat[F]
    )(implicit snapshotSerializer: SnapshotSerializer[F, A], snapshotReadWrite: SnapshotReadWrite[F, A]) =
      SnapshotStoreAdapter(store, toKey, origin)

    for {
      store <- SnapshotCassandra.of(config, origin, cassandraClusterOf)
    } yield adapter(store)(snapshotSerializer, snapshotReadWrite)
  }

  def apply[F[_]: Monad: Fail, A](store: SnapshotStoreFlat[F], toKey: ToKey[F], origin: Option[Origin])(implicit
    snapshotSerializer: SnapshotSerializer[F, A],
    snapshotReadWrite: SnapshotReadWrite[F, A]
  ): SnapshotStoreAdapter[F] =
    new SnapshotStoreAdapter[F] {

      def load(persistenceId: String, criteria: SnapshotSelectionCriteria): F[Option[SelectedSnapshot]] =
        for {
          key <- toKey(persistenceId)
          criteria <- convertCriteria(criteria)
          record <- store.load(key, criteria)
          snapshot <- record.traverse(deserializeSnapshot(persistenceId, _))
        } yield snapshot

      def save(metadata: SnapshotMetadata, snapshot: Any): F[Unit] =
        for {
          key <- toKey(metadata.persistenceId)
          snapshot <- serializeSnapshot(metadata, snapshot)
          _ <- store.save(key, snapshot)
        } yield ()

      def delete(metadata: SnapshotMetadata): F[Unit] =
        for {
          key <- toKey(metadata.persistenceId)
          seqNr <- SeqNr.of(metadata.sequenceNr)
          _ <- store.delete(key, seqNr)
        } yield ()

      def delete(persistenceId: String, criteria: SnapshotSelectionCriteria): F[Unit] =
        for {
          key <- toKey(persistenceId)
          criteria <- convertCriteria(criteria)
          _ <- store.delete(key, criteria)
        } yield ()

      def serializeSnapshot(metadata: SnapshotMetadata, snapshot: Any): F[SnapshotRecord[EventualPayloadAndType]] = {
        for {
          seqNr <- SeqNr.of(metadata.sequenceNr)
          snapshot <- snapshotSerializer.toInternalRepresentation(metadata, snapshot)
          payload <- snapshotReadWrite.eventualWrite(snapshot.payload)
          record = SnapshotRecord(
            snapshot = Snapshot(seqNr = seqNr, payload = payload),
            timestamp = Instant.ofEpochMilli(metadata.timestamp),
            origin = origin,
            version = Some(Version.current)
          )
        } yield record
      }

      def deserializeSnapshot(
        persistenceId: String,
        record: SnapshotRecord[EventualPayloadAndType]
      ): F[SelectedSnapshot] = {
        for {
          payload <- snapshotReadWrite.eventualRead(record.snapshot.payload)
          snapshot = record.snapshot.copy(payload = payload)
          snapshot <- snapshotSerializer.toAkkaRepresentation(persistenceId, record.timestamp, snapshot)
        } yield snapshot
      }

      def convertCriteria(criteria: SnapshotSelectionCriteria): F[journal.SnapshotSelectionCriteria] =
        for {
          maxSeqNr <- SeqNr.of(criteria.maxSequenceNr)
          maxTimestamp = Instant.ofEpochMilli(criteria.maxTimestamp)
          // this "if"" statement is required, because `0` is sometimes passed by Akka as a value here
          minSequenceNr <- if (criteria.minSequenceNr < 1) SeqNr.min.pure else SeqNr.of(criteria.minSequenceNr)
          minTimestamp = Instant.ofEpochMilli(criteria.minTimestamp)
        } yield journal.SnapshotSelectionCriteria(
          maxSeqNr = maxSeqNr,
          maxTimestamp = maxTimestamp,
          minSeqNr = minSequenceNr,
          minTimestamp = minTimestamp
        )

    }

  implicit class SnapshotStoreAdapterOps[F[_]](val self: SnapshotStoreAdapter[F]) extends AnyVal {

    def mapK[G[_]](fg: F ~> G, gf: G ~> F): SnapshotStoreAdapter[G] = new SnapshotStoreAdapter[G] {

      def load(persistenceId: String, criteria: SnapshotSelectionCriteria) =
        fg(self.load(persistenceId, criteria))

      def save(metadata: SnapshotMetadata, snapshot: Any) =
        fg(self.save(metadata, snapshot))

      def delete(metadata: SnapshotMetadata) =
        fg(self.delete(metadata))

      def delete(persistenceId: String, criteria: SnapshotSelectionCriteria) =
        fg(self.delete(persistenceId, criteria))

    }
  }

}
