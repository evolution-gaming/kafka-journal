package org.apache.pekko.persistence.kafka.journal.circe

import com.typesafe.config.ConfigFactory
import org.apache.pekko.persistence.journal.JournalSpec
import org.apache.pekko.persistence.kafka.journal.KafkaPluginSpec

class IntegrationCirceSpec extends JournalSpec(ConfigFactory.load("integration-circe.conf")) with KafkaPluginSpec {

  def supportsRejectingNonSerializableObjects = false
  override def supportsSerialization = false
}
