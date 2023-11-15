package akka.persistence.kafka.journal

import akka.persistence.{SelectedSnapshot, SnapshotMetadata}
import com.evolutiongaming.kafka.journal.Snapshot

trait SnapshotSerializer[F[_], A] {

  def toInternalRepresentation(metadata: SnapshotMetadata, snapshot: Any): F[Snapshot[A]]
  def toAkkaRepresentation(persistenceId: PersistenceId, snapshot: Snapshot[A]): F[SelectedSnapshot]

}
