package akka.persistence.kafka.journal

import com.evolutiongaming.kafka.journal.Key
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}

class ToKeySpec extends FunSuite with Matchers {

  test("apply from empty config") {
    val config = ConfigFactory.empty()
    val toKey = ToKey(config)
    toKey("id") shouldEqual Key(topic = "journal", id = "id")
  }

  test("apply from to-key-split config") {
    val config = ConfigFactory.parseURL(getClass.getResource("to-key-split.conf"))
    val toKey = ToKey(config)
    toKey("id-topic") shouldEqual Key(topic = "topic", id = "id")
    toKey("id") shouldEqual Key(topic = "test", id = "id")
  }

  test("apply from to-key-constant-topic config") {
    val config = ConfigFactory.parseURL(getClass.getResource("to-key-constant-topic.conf"))
    val toKey = ToKey(config)
    toKey("id-topic") shouldEqual Key(topic = "test", id = "id-topic")
    toKey("id") shouldEqual Key(topic = "test", id = "id")
  }
}
