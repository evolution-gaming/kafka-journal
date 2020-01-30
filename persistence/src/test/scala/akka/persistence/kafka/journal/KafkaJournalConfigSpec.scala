package akka.persistence.kafka.journal

import cats.implicits._
import com.evolutiongaming.kafka.journal.Journal.CallTimeThresholds
import com.evolutiongaming.kafka.journal.{JournalConfig, KafkaConfig}
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import pureconfig.{ConfigReader, ConfigSource}

import scala.concurrent.duration._

class KafkaJournalConfigSpec extends AnyFunSuite with Matchers {

  test("apply from empty config") {
    val config = ConfigFactory.empty()
    val expected = KafkaJournalConfig.default
    ConfigSource
      .fromConfig(config)
      .load[KafkaJournalConfig] shouldEqual expected.pure[ConfigReader.Result]
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
        delete = 4.millis),
      journal = JournalConfig(
        headCache = JournalConfig.HeadCache(enabled = false),
        kafka = KafkaConfig("client-id")),
      jsonCodec = KafkaJournalConfig.JsonCodec.Jsoniter,
    )
    ConfigSource
      .fromConfig(config)
      .load[KafkaJournalConfig] shouldEqual expected.pure[ConfigReader.Result]
  }

  test("apply from reference.conf") {
    val config = ConfigFactory.load()
    val expected = KafkaJournalConfig.default
    ConfigSource
      .fromConfig(config)
      .at("evolutiongaming.kafka-journal.persistence.journal")
      .load[KafkaJournalConfig] shouldEqual expected.pure[ConfigReader.Result]
  }
}
