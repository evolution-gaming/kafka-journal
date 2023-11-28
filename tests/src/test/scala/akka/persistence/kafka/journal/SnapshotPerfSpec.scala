package akka.persistence.kafka.journal

import akka.persistence.snapshot.SnapshotStorePerfSpec
import com.typesafe.config.ConfigFactory

class SnapshotPerfSpec extends SnapshotStorePerfSpec(ConfigFactory.load("perf.conf"))
  with KafkaPluginSpec {

  def supportsRejectingNonSerializableObjects = false
  override def supportsSerialization = false
  override def eventsCount = 100
  override def measurementIterations = 5
}
