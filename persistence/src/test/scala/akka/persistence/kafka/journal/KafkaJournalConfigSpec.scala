package akka.persistence.kafka.journal

import com.datastax.driver.core.ConsistencyLevel
import com.evolutiongaming.scassandra.{CassandraConfig, QueryConfig}
import com.evolutiongaming.kafka.journal.JournalConfig
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig
import com.evolutiongaming.skafka.CommonConfig
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig}
import com.evolutiongaming.skafka.producer.{Acks, ProducerConfig}
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
      startTimeout = 1.millis,
      stopTimeout = 2.seconds)
    KafkaJournalConfig(config) shouldEqual expected
  }

  test("apply from reference.conf") {
    val config = ConfigFactory.load().getConfig("evolutiongaming.kafka-journal.persistence.journal")
    val expected = KafkaJournalConfig(
      journal = JournalConfig(
        producer = ProducerConfig(
          common = CommonConfig(
            clientId = Some("journal"),
            sendBufferBytes = 1000000),
          acks = Acks.All,
          idempotence = true),
        consumer = ConsumerConfig(
          common = CommonConfig(
            clientId = Some("journal"),
            receiveBufferBytes = 100000),
          groupId = Some("journal"),
          maxPollRecords = 100,
          autoCommit = false,
          autoOffsetReset = AutoOffsetReset.Earliest)),
      cassandra = EventualCassandraConfig(
        client = CassandraConfig(
          name = "journal",
          query = QueryConfig(
            consistency = ConsistencyLevel.LOCAL_QUORUM,
            fetchSize = 100,
            defaultIdempotence = true))))
    KafkaJournalConfig(config) shouldEqual expected
  }
}
