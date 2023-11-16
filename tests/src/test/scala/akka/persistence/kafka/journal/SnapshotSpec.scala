package akka.persistence.kafka.journal

import akka.persistence.snapshot.SnapshotStoreSpec
import com.typesafe.config.ConfigFactory

class SnapshotSpec extends SnapshotStoreSpec(ConfigFactory.load("integration.conf")) {

  override def supportsSerialization = false

}
