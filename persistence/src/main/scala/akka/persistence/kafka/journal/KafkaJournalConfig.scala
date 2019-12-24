package akka.persistence.kafka.journal

import cats.implicits._
import com.evolutiongaming.config.ConfigHelper._
import com.evolutiongaming.kafka.journal.Journals.CallTimeThresholds
import com.evolutiongaming.kafka.journal.JournalConfig
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig
import com.typesafe.config.Config
import pureconfig.error.{ConfigReaderFailures, ThrowableFailure}
import pureconfig.{ConfigCursor, ConfigReader, ConfigSource}

import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.Try

final case class KafkaJournalConfig(
  journal: JournalConfig = JournalConfig.default,
  cassandra: EventualCassandraConfig = EventualCassandraConfig.default,
  startTimeout: FiniteDuration = 1.minute,
  stopTimeout: FiniteDuration = 1.minute,
  maxEventsInBatch: Int = 10,
  callTimeThresholds: CallTimeThresholds = CallTimeThresholds.default)

object KafkaJournalConfig {

  val default: KafkaJournalConfig = KafkaJournalConfig()


  implicit val configReaderKafkaJournalConfig: ConfigReader[KafkaJournalConfig] = {
    cursor: ConfigCursor => {
      for {
        cursor    <- cursor.asObjectCursor
        cassandra  = Try { fromConfig(cursor.value.toConfig, default) }
        cassandra <- cassandra.toEither.leftMap(a => ConfigReaderFailures(ThrowableFailure(a, cursor.location)))
      } yield cassandra
    }
  }


  def apply(config: Config): KafkaJournalConfig = apply(config, default)

  def apply(config: Config, default: => KafkaJournalConfig): KafkaJournalConfig = fromConfig(config, default)

  private def fromConfig(config: Config, default: => KafkaJournalConfig): KafkaJournalConfig = {

    def get[T: FromConf](name: String) = config.getOpt[T](name)

    val callTimeThresholds = ConfigSource.fromConfig(config)
      .at("call-time-thresholds")
      .load[CallTimeThresholds]
      .getOrElse(CallTimeThresholds.default)

    val source = ConfigSource.fromConfig(config)

    def getOrThrow[A : ConfigReader : ClassTag](name: String) = {
      val source1 = source.at(name)
      source1.value().fold(_ => none[A], _ => source1.loadOrThrow[A].some)
    }

    KafkaJournalConfig(
      journal = JournalConfig.fromConfig(config, default.journal),
      cassandra = getOrThrow[EventualCassandraConfig]("cassandra") getOrElse default.cassandra,
      startTimeout = get[FiniteDuration]("start-timeout") getOrElse default.startTimeout,
      stopTimeout = get[FiniteDuration]("stop-timeout") getOrElse default.stopTimeout,
      maxEventsInBatch = get[Int]("max-events-in-batch") getOrElse default.maxEventsInBatch,
      callTimeThresholds = callTimeThresholds)
  }
}