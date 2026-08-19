package com.evolution.kafka.journal.it.pekko

import org.apache.pekko.persistence.JournalCapabilityFlags

/**
 * Shared [[JournalCapabilityFlags]] for pekko-persistence-tck-based integration test suites testing
 * Kafka-Journal.
 */
private[pekko] trait KafkaJournalCapabilityFlags extends JournalCapabilityFlags {
  override final def supportsRejectingNonSerializableObjects = false
  override final def supportsSerialization = false
}
