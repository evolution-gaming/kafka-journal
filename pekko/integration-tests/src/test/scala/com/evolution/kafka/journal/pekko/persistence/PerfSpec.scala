package com.evolution.kafka.journal.pekko.persistence

import com.typesafe.config.ConfigFactory
import org.apache.pekko.persistence.journal.JournalPerfSpec

class PerfSpec extends JournalPerfSpec(ConfigFactory.load("perf.conf")) with KafkaPluginSpec {

  def supportsRejectingNonSerializableObjects = false
  override def supportsSerialization = false
  override def eventsCount = 100
  override def measurementIterations = 5
}
