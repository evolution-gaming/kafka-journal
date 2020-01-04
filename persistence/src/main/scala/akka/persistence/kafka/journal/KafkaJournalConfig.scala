package akka.persistence.kafka.journal

import com.evolutiongaming.kafka.journal.Journal.CallTimeThresholds
import com.evolutiongaming.kafka.journal.JournalConfig
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration._

final case class KafkaJournalConfig(
  journal: JournalConfig = JournalConfig.default,
  cassandra: EventualCassandraConfig = EventualCassandraConfig.default,
  startTimeout: FiniteDuration = 1.minute,
  stopTimeout: FiniteDuration = 1.minute,
  maxEventsInBatch: Int = 10,
  callTimeThresholds: CallTimeThresholds = CallTimeThresholds.default)

object KafkaJournalConfig {

  val default: KafkaJournalConfig = KafkaJournalConfig()

  implicit val configReaderKafkaJournalConfig: ConfigReader[KafkaJournalConfig] = deriveReader
}