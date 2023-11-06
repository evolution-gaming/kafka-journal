package akka.persistence.kafka.journal

import akka.persistence.snapshot.SnapshotStore
import akka.persistence.{
  SelectedSnapshot,
  SnapshotMetadata,
  SnapshotSelectionCriteria
}
import com.typesafe.config.Config

import scala.concurrent.Future

class CassandraSnapshotStore(config: Config) extends SnapshotStore { actor =>

  override def loadAsync(
    persistenceId: String,
    criteria: SnapshotSelectionCriteria
  ): Future[Option[SelectedSnapshot]] = ???

  override def saveAsync(
    metadata: SnapshotMetadata,
    snapshot: Any
  ): Future[Unit] = ???

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = ???

  override def deleteAsync(
    persistenceId: String,
    criteria: SnapshotSelectionCriteria
  ): Future[Unit] = ???

}
