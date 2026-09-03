package com.evolution.kafka.journal.it.pekko

import com.evolution.kafka.journal.it.IntegrationSuite
import org.apache.pekko.persistence.PluginSpec
import org.apache.pekko.testkit.DefaultTimeout

private[pekko] trait KafkaPluginSpec extends PluginSpec with DefaultTimeout {

  override def beforeAll(): Unit = {
    IntegrationSuite.start()
    super.beforeAll()
  }
}
