package com.evolution.kafka.journal.it.akka

import akka.persistence.journal.JournalPerfSpec
import com.typesafe.config.ConfigFactory

class AkkaJournalPluginPerfSpec extends JournalPerfSpec(ConfigFactory.load("perf.conf")) with KafkaPluginSpec {

  def supportsRejectingNonSerializableObjects = false
  override def supportsSerialization = false
  override def eventsCount = 100
  override def measurementIterations = 5
}
