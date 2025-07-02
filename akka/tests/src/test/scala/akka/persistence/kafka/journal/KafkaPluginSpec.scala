package akka.persistence.kafka.journal

import akka.persistence.PluginSpec
import akka.testkit.DefaultTimeout
import com.evolution.kafka.journal.IntegrationSuite

trait KafkaPluginSpec extends PluginSpec with DefaultTimeout {

  override def beforeAll(): Unit = {
    IntegrationSuite.start()
    super.beforeAll()
  }
}
