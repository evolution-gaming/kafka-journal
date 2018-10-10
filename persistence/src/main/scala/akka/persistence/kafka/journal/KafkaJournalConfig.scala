package akka.persistence.kafka.journal

import com.evolutiongaming.config.ConfigHelper._
import com.evolutiongaming.kafka.journal.JournalConfig
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig
import com.typesafe.config.Config

import scala.concurrent.duration._

final case class KafkaJournalConfig(
  journal: JournalConfig = JournalConfig.Default,
  cassandra: EventualCassandraConfig = EventualCassandraConfig.Default,
  blockingDispatcher: String = "evolutiongaming.kafka-journal.persistence.blocking-dispatcher",
  connectTimeout: FiniteDuration = 5.seconds,
  stopTimeout: FiniteDuration = 10.seconds)

object KafkaJournalConfig {

  val Default: KafkaJournalConfig = KafkaJournalConfig()

  def apply(config: Config): KafkaJournalConfig = {
    apply(config, Default)
  }

  def apply(config: Config, default: => KafkaJournalConfig): KafkaJournalConfig = {

    def get[T: FromConf](name: String) = config.getOpt[T](name)

    KafkaJournalConfig(
      journal = JournalConfig(config),
      cassandra = get[Config]("cassandra").fold(default.cassandra)(EventualCassandraConfig(_)),
      blockingDispatcher = get[String]("blocking-dispatcher") getOrElse default.blockingDispatcher,
      connectTimeout = get[FiniteDuration]("connect-timeout") getOrElse default.connectTimeout,
      stopTimeout = get[FiniteDuration]("stop-timeout") getOrElse default.stopTimeout)
  }
}