package akka.persistence.kafka.journal

import akka.persistence.PluginSpec
import akka.testkit.DefaultTimeout
import com.evolutiongaming.kafka.journal.IntegrationSuit

trait KafkaPluginSpec extends PluginSpec with DefaultTimeout {

  override def beforeAll(): Unit = {
    IntegrationSuit.start()
    super.beforeAll()
  }
}