package akka.persistence.kafka.journal

import akka.persistence.snapshot.SnapshotStoreSpec
import com.typesafe.config.ConfigFactory

class SnapshotSpec extends SnapshotStoreSpec(ConfigFactory.load("integration.conf")) with KafkaPluginSpec {

  override def supportsSerialization = false

}
