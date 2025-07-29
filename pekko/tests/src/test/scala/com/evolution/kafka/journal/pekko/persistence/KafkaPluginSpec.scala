package com.evolution.kafka.journal.pekko.persistence

import com.evolution.kafka.journal.IntegrationSuite
import org.apache.pekko.persistence.PluginSpec
import org.apache.pekko.testkit.DefaultTimeout

trait KafkaPluginSpec extends PluginSpec with DefaultTimeout {

  override def beforeAll(): Unit = {
    IntegrationSuite.start()
    super.beforeAll()
  }
}
