package akka.persistence.kafka.journal

import com.evolutiongaming.kafka.journal.Journal.CallTimeThresholds
import com.evolutiongaming.kafka.journal.JournalConfig
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig
import pureconfig.generic.semiauto.{deriveEnumerationReader, deriveReader}
import pureconfig.{ConfigCursor, ConfigReader, ConfigSource, Derivation}

import scala.concurrent.duration._

final case class KafkaJournalConfig(
  journal: JournalConfig = JournalConfig.default,
  cassandra: EventualCassandraConfig = EventualCassandraConfig.default,
  startTimeout: FiniteDuration = 1.minute,
  stopTimeout: FiniteDuration = 1.minute,
  maxEventsInBatch: Int = 500,
  callTimeThresholds: CallTimeThresholds = CallTimeThresholds.default,
  jsonCodec: KafkaJournalConfig.JsonCodec = KafkaJournalConfig.JsonCodec.Default)

object KafkaJournalConfig {

  val default: KafkaJournalConfig = KafkaJournalConfig()

  implicit val configReaderKafkaJournalConfig: ConfigReader[KafkaJournalConfig] = {

    val configReader = deriveReader[KafkaJournalConfig]

    cursor: ConfigCursor => {
      for {
        cursor  <- cursor.asObjectCursor
        config   = cursor.value.toConfig
        source   = ConfigSource.fromConfig(config)
        config  <- source.load(Derivation.Successful(configReader))
        journal <- source.load[JournalConfig]
      } yield {
        config.copy(journal = journal)
      }
    }
  }

  sealed trait JsonCodec

  object JsonCodec {
    case object Default  extends JsonCodec
    case object PlayJson extends JsonCodec
    case object Jsoniter extends JsonCodec

    implicit val configReaderJsonCodec: ConfigReader[JsonCodec] = deriveEnumerationReader
  }
}