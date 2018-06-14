package akka.persistence.kafka.journal

import java.util.UUID

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory

class KafkaJournalSpec extends JournalSpec(ConfigFactory.load()) {

  var shutdownKafka: StartKafka.Shutdown = StartKafka.Shutdown.Empty

  override def beforeAll(): Unit = {
    super.beforeAll()
    shutdownKafka = StartKafka()
  }

  private var _pid: String = _

  protected override def beforeEach() = {
    _pid = UUID.randomUUID().toString
    super.beforeEach()
  }

  override def pid = _pid

  def supportsRejectingNonSerializableObjects = CapabilityFlag.off()

  override def afterAll(): Unit = {
    shutdownKafka()
    super.afterAll()
  }
}
