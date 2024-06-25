package akka.persistence.kafka.journal

import cats.Id
import com.evolutiongaming.kafka.journal.Key
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ToKeySpec extends AnyFunSuite with Matchers {

  test("apply from empty config") {
    val config = ConfigFactory.empty()
    val toKey  = ToKey.fromConfig[Id](config)
    toKey("id") shouldEqual Key(topic = "journal", id = "id")
  }

  test("apply from to-key-split config") {
    val config = ConfigFactory.parseURL(getClass.getResource("to-key-split.conf"))
    val toKey  = ToKey.fromConfig[Id](config)
    toKey("topic-id") shouldEqual Key(id = "id", topic = "topic")
    toKey("t-o-p-i-c-id") shouldEqual Key(id = "id", topic = "t-o-p-i-c")
    toKey("id") shouldEqual Key(id = "id", topic = "test")
  }

  test("apply from to-key-constant-topic config") {
    val config = ConfigFactory.parseURL(getClass.getResource("to-key-constant-topic.conf"))
    val toKey  = ToKey.fromConfig[Id](config)
    toKey("topic-id") shouldEqual Key(topic = "test", id = "topic-id")
    toKey("id") shouldEqual Key(id = "id", topic = "test")
  }
}
