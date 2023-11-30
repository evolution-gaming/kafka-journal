package akka.persistence.kafka.journal

import akka.persistence.snapshot.SnapshotStorePerfSpec
import com.typesafe.config.ConfigFactory

class LegacySnapshotPerfSpec extends SnapshotStorePerfSpec(ConfigFactory.load("snapshot-legacy-perf.conf"))
  with KafkaPluginSpec {

  def supportsRejectingNonSerializableObjects = false
  override def supportsSerialization = false
  override def eventsCount = 100
  override def snapshotPerEvents = 1
  override def measurementIterations = 5
}
