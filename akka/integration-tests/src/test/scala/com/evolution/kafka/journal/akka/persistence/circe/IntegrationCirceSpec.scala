package com.evolution.kafka.journal.akka.persistence.circe

import akka.persistence.journal.JournalSpec
import com.evolution.kafka.journal.akka.persistence.KafkaPluginSpec
import com.typesafe.config.ConfigFactory

class IntegrationCirceSpec extends JournalSpec(ConfigFactory.load("integration-circe.conf")) with KafkaPluginSpec {

  def supportsRejectingNonSerializableObjects = false
  override def supportsSerialization = false
}
