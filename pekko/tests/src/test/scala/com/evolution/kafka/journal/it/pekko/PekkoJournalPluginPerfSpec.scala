package com.evolution.kafka.journal.it.pekko

import com.evolution.kafka.journal.it.SharedItEnv
import org.apache.pekko.persistence.journal.JournalPerfSpec

class PekkoJournalPluginPerfSpec extends JournalPerfSpec(
  config = SharedItEnv.require().loadSuiteConfig[PekkoJournalPluginPerfSpec].unparsed,
) with KafkaJournalCapabilityFlags {
  override def eventsCount = 100
  override def measurementIterations = 5
}
