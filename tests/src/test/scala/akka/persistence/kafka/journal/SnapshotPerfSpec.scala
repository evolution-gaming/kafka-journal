package akka.persistence.kafka.journal

import akka.persistence.snapshot.SnapshotStorePerfSpec
import com.typesafe.config.ConfigFactory

class SnapshotPerfSpec extends SnapshotStorePerfSpec(ConfigFactory.load("snapshot.conf"))
  with KafkaPluginSpec {

  def supportsRejectingNonSerializableObjects = false
  override def supportsSerialization = false
  override def eventsCount = 100
  override def snapshotPerEvents = 1
  override def measurementIterations = 5
}
