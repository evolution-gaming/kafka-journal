package akka.persistence.kafka.journal

import akka.persistence.{SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria}
import cats.Monad
import cats.effect.kernel.Resource
import cats.syntax.all._
import com.evolutiongaming.kafka.journal
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.EventualPayloadAndType
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

  def of[F[_], A](
    toKey: ToKey[F],
    origin: Option[Origin],
    snapshotSerializer: SnapshotSerializer[F, A],
    snapshotReadWrite: SnapshotReadWrite[F, A],
    config: KafkaJournalConfig,
    cassandraClusterOf: CassandraClusterOf[F]
  ): Resource[F, SnapshotStoreAdapter[F]] = ???

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
          _ <- store.drop(key, seqNr)
        } yield ()

      def delete(persistenceId: String, criteria: SnapshotSelectionCriteria): F[Unit] =
        for {
          key <- toKey(persistenceId)
          criteria <- convertCriteria(criteria)
          _ <- store.drop(key, criteria)
        } yield ()

      def serializeSnapshot(metadata: SnapshotMetadata, snapshot: Any): F[SnapshotRecord[EventualPayloadAndType]] = {
        for {
          seqNr <- SeqNr.of(metadata.sequenceNr)
          snapshot <- snapshotSerializer.toInternalRepresentation(metadata, snapshot)
          payload <- snapshot.payload.traverse { payload =>
            snapshotReadWrite.eventualWrite(payload)
          }
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
          payload <- record.snapshot.payload.traverse { payloadAndType =>
            snapshotReadWrite.eventualRead(payloadAndType)
          }
          snapshot = record.snapshot.copy(payload = payload)
          snapshot <- snapshotSerializer.toAkkaRepresentation(persistenceId, snapshot)
        } yield snapshot
      }

      def convertCriteria(criteria: SnapshotSelectionCriteria): F[journal.SnapshotSelectionCriteria] =
        for {
          maxSeqNr <- SeqNr.of(criteria.maxSequenceNr)
          maxTimestamp = Instant.ofEpochMilli(criteria.maxTimestamp)
          minSequenceNr <- SeqNr.of(criteria.minSequenceNr)
          minTimestamp = Instant.ofEpochMilli(criteria.minTimestamp)
        } yield journal.SnapshotSelectionCriteria(
          maxSeqNr = maxSeqNr,
          maxTimestamp = maxTimestamp,
          minSeqNr = minSequenceNr,
          minTimestamp = minTimestamp
        )

    }

}
