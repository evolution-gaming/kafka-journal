package akka.persistence.kafka.journal

import cats.Applicative
import cats.implicits._
import com.evolutiongaming.config.ConfigHelper._
import com.evolutiongaming.kafka.journal.Key
import com.evolutiongaming.skafka.Topic
import com.typesafe.config.Config

trait ToKey[F[_]] {

  def apply(persistenceId: PersistenceId): F[Key]
}

object ToKey {

  def default[F[_] : Applicative]: ToKey[F] = constantTopic("journal")


  def constantTopic[F[_] : Applicative](topic: Topic): ToKey[F] = {
    persistenceId: PersistenceId => Key(topic = topic, id = persistenceId).pure[F]
  }


  def fromConfig[F[_] : Applicative](config: Config): ToKey[F] = {
    fromConfig[F](config, default[F])
  }

  def fromConfig[F[_] : Applicative](config: Config, default: => ToKey[F]): ToKey[F] = {

    def apply(config: Config) = {

      def onSplit() = {
        val separator = config.getOpt[String]("split.separator") getOrElse "-"
        val fallback = apply("split.fallback")
        ToKey.split[F](separator, fallback)
      }

      def onConstantTopic() = {
        config.getOpt[String]("constant-topic.topic").fold(default)(constantTopic)
      }

      def apply(name: String): ToKey[F] = {
        config.getOpt[String](name).fold(default) {
          case "constant-topic" => onConstantTopic()
          case "split"          => onSplit()
        }
      }

      apply("impl")
    }

    config.getOpt[Config]("persistence-id-to-key").fold(default)(apply)
  }


  def split[F[_] : Applicative](separator: String, fallback: ToKey[F]): ToKey[F] = {
    persistenceId: PersistenceId => {
      persistenceId.lastIndexOf(separator) match {
        case -1  => fallback(persistenceId)
        case idx =>
          val topic = persistenceId.take(idx)
          val id = persistenceId.drop(idx + separator.length)
          Key(topic = topic, id = id).pure[F]
      }
    }
  }
}