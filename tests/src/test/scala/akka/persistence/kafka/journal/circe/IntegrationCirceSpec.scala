package akka.persistence.kafka.journal.circe

import akka.persistence.journal.JournalSpec
import akka.persistence.kafka.journal.KafkaPluginSpec
import com.typesafe.config.ConfigFactory

class IntegrationCirceSpec extends JournalSpec(ConfigFactory.load("integration-circe.conf")) with KafkaPluginSpec {

  def supportsRejectingNonSerializableObjects = false
  override def supportsSerialization          = false
}
