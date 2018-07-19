package akka.persistence.kafka.journal

import com.evolutiongaming.kafka.journal.Key
import com.evolutiongaming.skafka.Topic

trait ToKey {
  def apply(persistenceId: String): Key
}

object ToKey {
  val Identity: ToKey = new ToKey {
    def apply(persistenceId: String) = Key(id = persistenceId, topic = persistenceId)
  }

  def apply(topic: Topic): ToKey = new ToKey {
    def apply(persistenceId: String) = Key(id = persistenceId, topic = topic)
  }
}