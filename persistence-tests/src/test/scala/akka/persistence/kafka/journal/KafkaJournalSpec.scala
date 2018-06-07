package akka.persistence.kafka.journal

import java.util.UUID

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory

class KafkaJournalSpec extends JournalSpec(ConfigFactory.load()) {

  private var _pid: String = _

  protected override def beforeEach() = {
    _pid = UUID.randomUUID().toString
    super.beforeEach()
  }

  override def pid = _pid

  def supportsRejectingNonSerializableObjects = CapabilityFlag.off()
}
