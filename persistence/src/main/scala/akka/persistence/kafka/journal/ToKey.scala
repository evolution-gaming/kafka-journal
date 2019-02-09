package akka.persistence.kafka.journal

import com.evolutiongaming.config.ConfigHelper._
import com.evolutiongaming.kafka.journal.Key
import com.evolutiongaming.skafka.Topic
import com.typesafe.config.Config

trait ToKey {
  def apply(persistenceId: PersistenceId): Key
}

object ToKey {
  val Default: ToKey = ToKey("journal")

  val Identity: ToKey = new ToKey {
    def apply(persistenceId: PersistenceId) = Key(topic = persistenceId, id = persistenceId)
  }

  def apply(topic: Topic): ToKey = new ToKey {
    def apply(persistenceId: PersistenceId) = Key(topic = topic, id = persistenceId)
  }

  def apply(config: Config): ToKey = {

    def apply(config: Config) = {

      def onSplit() = {
        val separator = config.getOpt[String]("split.separator") getOrElse "-"
        val fallback = apply("split.fallback")
        ToKey.split(separator, fallback)
      }

      def onConstantTopic() = {
        config.getOpt[String]("constant-topic.topic").fold(Default) { topic => ToKey(topic) }
      }

      def apply(name: String): ToKey = {
        config.getOpt[String](name).fold(Default) {
          case "constant-topic" => onConstantTopic()
          case "split"          => onSplit()
        }
      }

      apply("impl")
    }

    config.getOpt[Config]("persistence-id-to-key").fold(Default)(apply)
  }

  def split(separator: String, fallback: ToKey): ToKey = new ToKey {

    def apply(persistenceId: PersistenceId): Key = {
      persistenceId.lastIndexOf(separator) match {
        case -1  => fallback(persistenceId)
        case idx =>
          val topic = persistenceId.take(idx)
          val id = persistenceId.drop(idx + separator.length)
          Key(topic = topic, id = id)
      }
    }
  }
}