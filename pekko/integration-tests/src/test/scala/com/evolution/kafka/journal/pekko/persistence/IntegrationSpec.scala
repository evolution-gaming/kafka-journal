package com.evolution.kafka.journal.pekko.persistence

import com.typesafe.config.ConfigFactory
import org.apache.pekko.persistence.journal.JournalSpec

class IntegrationSpec extends JournalSpec(ConfigFactory.load("integration.conf")) with KafkaPluginSpec {

  def supportsRejectingNonSerializableObjects = false
  override def supportsSerialization = false
}
