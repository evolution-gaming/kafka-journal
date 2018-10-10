package akka.persistence.kafka.journal

import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration._

class KafkaJournalConfigSpec extends FunSuite with Matchers {

  test("apply from empty config") {
    val config = ConfigFactory.empty()
    KafkaJournalConfig(config) shouldEqual KafkaJournalConfig.Default
  }

  test("apply from config") {
    val config = ConfigFactory.parseURL(getClass.getResource("kafka-journal.conf"))
    val expected = KafkaJournalConfig(
      blockingDispatcher = "blocking-dispatcher",
      connectTimeout = 1.millis,
      stopTimeout = 2.seconds)
    KafkaJournalConfig(config) shouldEqual expected
  }
}
