package com.evolution.kafka.journal.it.akka

import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory

class AkkaJournalPluginCirceSpec extends JournalSpec(ConfigFactory.load("integration-circe.conf"))
with KafkaPluginSpec {

  def supportsRejectingNonSerializableObjects = false
  override def supportsSerialization = false
}
