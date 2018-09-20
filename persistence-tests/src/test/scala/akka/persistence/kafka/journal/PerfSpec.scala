package akka.persistence.kafka.journal

import akka.persistence.journal.JournalPerfSpec
import com.typesafe.config.ConfigFactory

class PerfSpec extends JournalPerfSpec(ConfigFactory.load("perf.conf"))
  with KafkaPluginSpec {

  def supportsRejectingNonSerializableObjects = false
  override def supportsSerialization = false
  override def eventsCount: Int = 10 // TODO remove this
}