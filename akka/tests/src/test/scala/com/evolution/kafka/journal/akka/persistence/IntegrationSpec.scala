package com.evolution.kafka.journal.akka.persistence

import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory

class IntegrationSpec extends JournalSpec(ConfigFactory.load("integration.conf")) with KafkaPluginSpec {

  def supportsRejectingNonSerializableObjects = false
  override def supportsSerialization = false
}
