package akka.persistence.kafka.journal

import cats.Monad
import cats.syntax.all._
import com.evolutiongaming.kafka.journal.eventual.EventualPayloadAndType
import com.evolutiongaming.kafka.journal.{PayloadAndType, SeqNr, SnapshotRecord, SnapshotSelectionCriteria, SnapshotStoreFlat}

trait SnapshotStoreAdapter[F[_]] {

  def load(
    persistenceId: PersistenceId,
    criteria: SnapshotSelectionCriteria
  ): F[Option[SnapshotRecord[EventualPayloadAndType]]]

  def save(persistenceId: PersistenceId, snapshot: SnapshotRecord[PayloadAndType]): F[Unit]

  def delete(persistenceId: String, seqNr: SeqNr): F[Unit]

  def delete(persistenceId: String, criteria: SnapshotSelectionCriteria): F[Unit]

}

object SnapshotStoreAdapter {

  def apply[F[_]: Monad](store: SnapshotStoreFlat[F], toKey: ToKey[F]): SnapshotStoreAdapter[F] =
    new SnapshotStoreAdapter[F] {

      def load(
        persistenceId: String,
        criteria: SnapshotSelectionCriteria
      ): F[Option[SnapshotRecord[EventualPayloadAndType]]] =
        for {
          key <- toKey(persistenceId)
          snapshot <- store.load(key, criteria)
        } yield snapshot

      def save(persistenceId: PersistenceId, snapshot: SnapshotRecord[PayloadAndType]): F[Unit] = ???

      def delete(persistenceId: String, seqNr: SeqNr): F[Unit] =
        toKey(persistenceId).flatMap { key =>
          store.drop(key, seqNr)
        }

      def delete(persistenceId: String, criteria: SnapshotSelectionCriteria): F[Unit] =
        toKey(persistenceId).flatMap { key =>
          store.drop(key, criteria)
        }

    }

}
