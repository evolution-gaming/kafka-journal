package com.evolution.kafka.journal.it.akka

import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory

class AkkaJournalPluginSpec extends JournalSpec(ConfigFactory.load("integration.conf")) with KafkaPluginSpec {

  def supportsRejectingNonSerializableObjects = false
  override def supportsSerialization = false
}
