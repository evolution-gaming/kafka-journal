package com.evolution.kafka.journal.it.akka

import akka.persistence.journal.JournalPerfSpec
import com.evolution.kafka.journal.it.SharedItEnv

class AkkaJournalPluginPerfSpec extends JournalPerfSpec(
  config = SharedItEnv.require().loadSuiteConfig[AkkaJournalPluginPerfSpec].unparsed,
) with KafkaJournalCapabilityFlags {
  override def eventsCount = 100
  override def measurementIterations = 5
}
