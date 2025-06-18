package akka.persistence.kafka.journal

import cats.Applicative
import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.Key
import com.evolutiongaming.skafka.Topic
import com.typesafe.config.Config
import pureconfig.{ConfigCursor, ConfigReader, ConfigSource}

trait ToKey[F[_]] {

  def apply(persistenceId: PersistenceId): F[Key]
}

object ToKey {

  def default[F[_]: Applicative]: ToKey[F] = constantTopic("journal")

  def constantTopic[F[_]: Applicative](topic: Topic): ToKey[F] = { (persistenceId: PersistenceId) =>
    Key(topic = topic, id = persistenceId).pure[F]
  }

  def split[F[_]: Applicative](separator: String, fallback: ToKey[F]): ToKey[F] = { (persistenceId: PersistenceId) =>
    {
      persistenceId.lastIndexOf(separator) match {
        case -1 => fallback(persistenceId)
        case idx =>
          val topic = persistenceId.take(idx)
          val id = persistenceId.drop(idx + separator.length)
          Key(topic = topic, id = id).pure[F]
      }
    }
  }

  implicit def configReaderReplicatorConfig[F[_]: Applicative]: ConfigReader[ToKey[F]] = { (cursor: ConfigCursor) =>
    {
      cursor
        .asObjectCursor
        .flatMap { cursor =>
          val source = ConfigSource.fromConfig(cursor.objValue.toConfig)

          def onSplit = {
            val separator = source
              .at("split.separator")
              .load[String]
              .getOrElse("-")
            val fallback = apply("split.fallback") getOrElse default[F]
            ToKey.split[F](separator, fallback)
          }

          def onConstantTopic = {
            source
              .at("constant-topic.topic")
              .load[String]
              .map { a => constantTopic[F](a) }
          }

          def apply(name: String): ConfigReader.Result[ToKey[F]] = {
            source
              .at(name)
              .load[String]
              .flatMap {
                case "constant-topic" => onConstantTopic
                case "split" => onSplit.pure[ConfigReader.Result]
              }
          }

          apply("impl")
        }
    }
  }

  def fromConfig[F[_]: Applicative](config: Config): ToKey[F] = {
    fromConfig[F](config, default[F])
  }

  def fromConfig[F[_]: Applicative](config: Config, default: => ToKey[F]): ToKey[F] = {
    ConfigSource
      .fromConfig(config)
      .at("persistence-id-to-key")
      .load[ToKey[F]]
      .getOrElse(default)
  }
}
