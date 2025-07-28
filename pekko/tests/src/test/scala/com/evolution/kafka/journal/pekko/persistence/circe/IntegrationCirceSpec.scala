package com.evolution.kafka.journal.pekko.persistence.circe

import com.typesafe.config.ConfigFactory
import org.apache.pekko.persistence.journal.JournalSpec
import com.evolution.kafka.journal.pekko.persistence.KafkaPluginSpec

class IntegrationCirceSpec extends JournalSpec(ConfigFactory.load("integration-circe.conf")) with KafkaPluginSpec {

  def supportsRejectingNonSerializableObjects = false
  override def supportsSerialization = false
}
