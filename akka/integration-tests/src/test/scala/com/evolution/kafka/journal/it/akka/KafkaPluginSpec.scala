package com.evolution.kafka.journal.it.akka

import akka.persistence.PluginSpec
import akka.testkit.DefaultTimeout
import com.evolution.kafka.journal.it.IntegrationSuite

private[akka] trait KafkaPluginSpec extends PluginSpec with DefaultTimeout {

  override def beforeAll(): Unit = {
    IntegrationSuite.start()
    super.beforeAll()
  }
}
