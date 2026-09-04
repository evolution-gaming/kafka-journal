package com.evolution.kafka.journal.it.akka

import akka.persistence.JournalCapabilityFlags

/**
 * Shared [[JournalCapabilityFlags]] for akka-persistence-tck-based integration test suites testing
 * Kafka-Journal.
 */
private[akka] trait KafkaJournalCapabilityFlags extends JournalCapabilityFlags {
  override final def supportsRejectingNonSerializableObjects = false
  override final def supportsSerialization = false
}
