package akka.persistence.kafka.journal

import com.evolutiongaming.kafka.journal.Journals.CallTimeThresholds
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class KafkaJournalConfigSpec extends AnyFunSuite with Matchers {

  test("apply from empty config") {
    val config = ConfigFactory.empty()
    KafkaJournalConfig(config) shouldEqual KafkaJournalConfig.default
  }

  test("apply from config") {
    val config = ConfigFactory.parseURL(getClass.getResource("kafka-journal.conf"))
    val expected = KafkaJournalConfig(
      startTimeout = 1.millis,
      stopTimeout = 2.seconds,
      maxEventsInBatch = 3,
      callTimeThresholds = CallTimeThresholds(
        append = 1.millis,
          read = 2.millis,
          pointer = 3.millis,
          delete = 4.millis))
    KafkaJournalConfig(config) shouldEqual expected
  }

  test("apply from reference.conf") {
    val config = ConfigFactory.load().getConfig("evolutiongaming.kafka-journal.persistence.journal")
    val expected = KafkaJournalConfig.default
    KafkaJournalConfig(config) shouldEqual expected
  }
}
