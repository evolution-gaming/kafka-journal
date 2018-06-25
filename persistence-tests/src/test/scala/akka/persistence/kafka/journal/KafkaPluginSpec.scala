package akka.persistence.kafka.journal

import akka.persistence.PluginSpec
import com.evolutiongaming.kafka.StartKafka

trait KafkaPluginSpec extends PluginSpec {

  var shutdownKafka: StartKafka.Shutdown = StartKafka.Shutdown.Empty

  override def beforeAll(): Unit = {
    shutdownKafka = StartKafka()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    shutdownKafka()
  }
}

