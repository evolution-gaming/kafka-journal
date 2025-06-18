package org.apache.pekko.persistence.kafka.journal

import com.evolutiongaming.kafka.journal.IntegrationSuite
import org.apache.pekko.persistence.PluginSpec
import org.apache.pekko.testkit.DefaultTimeout

trait KafkaPluginSpec extends PluginSpec with DefaultTimeout {

  override def beforeAll(): Unit = {
    IntegrationSuite.start()
    super.beforeAll()
  }
}
