package com.evolution.kafka.journal.pekko.persistence.circe

import com.evolution.kafka.journal.pekko.persistence.KafkaPluginSpec
import com.typesafe.config.ConfigFactory
import org.apache.pekko.persistence.journal.JournalSpec

class IntegrationCirceSpec extends JournalSpec(ConfigFactory.load("integration-circe.conf")) with KafkaPluginSpec {

  def supportsRejectingNonSerializableObjects = false
  override def supportsSerialization = false
}
