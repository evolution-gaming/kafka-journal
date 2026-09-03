package com.evolution.kafka.journal.it.pekko

import com.typesafe.config.ConfigFactory
import org.apache.pekko.persistence.journal.JournalSpec

class PekkoJournalPluginCirceSpec extends JournalSpec(ConfigFactory.load("integration-circe.conf"))
with KafkaPluginSpec {

  def supportsRejectingNonSerializableObjects = false
  override def supportsSerialization = false
}
