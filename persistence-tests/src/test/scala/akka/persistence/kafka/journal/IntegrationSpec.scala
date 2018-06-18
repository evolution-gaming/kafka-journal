package akka.persistence.kafka.journal

import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory

class IntegrationSpec extends JournalSpec(ConfigFactory.load()) with KafkaPluginSpec {

  def supportsRejectingNonSerializableObjects = false
}