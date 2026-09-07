package com.evolution.kafka.journal.it.pekko

import com.evolution.kafka.journal.it.SharedItEnv
import org.apache.pekko.persistence.journal.JournalSpec

import scala.reflect.ClassTag

/**
 * Base [[JournalSpec]] for Kafka-Journal persistence plugin.
 *
 * It requires [[SharedItEnv]] in the constructor and loads additional test suite specific config
 * overrides from a classpath resource file named `[test-suite-class-name].conf`.
 *
 * @tparam S
 *   concrete child test suite type - used to infer the class name, circumvents this problem:
 *   [[https://stackoverflow.com/questions/3806009/scala-how-to-get-the-class-in-its-own-constructor]]
 */
private[pekko] abstract class BasePekkoJournalPluginSpec[S <: BasePekkoJournalPluginSpec[S]: ClassTag]
extends JournalSpec(
  config = SharedItEnv.require().loadSuiteConfig[S].unparsed,
) with KafkaJournalCapabilityFlags
