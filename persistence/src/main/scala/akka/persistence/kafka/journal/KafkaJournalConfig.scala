package akka.persistence.kafka.journal

import com.evolutiongaming.kafka.journal.Journal.{CallTimeThresholds, ConsumerPoolConfig}
import com.evolutiongaming.kafka.journal.JournalConfig
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig
import pureconfig.generic.semiauto.{deriveEnumerationReader, deriveReader}
import pureconfig.{ConfigCursor, ConfigReader, ConfigSource}

import scala.concurrent.duration._

/** Configuration for [[KafkaJournal]].
  *
  * This case class specifies configuration that could be set using
  * `application.conf` (see `reference.conf` for an example of such
  * configuration).
  *
  * @param journal
  *   Kafka-specific configuration used by a plugin.
  * @param cassandra
  *   Cassandra-specific configuration used by a plugin.
  * @param startTimeout
  *   The timeout to create a journal adapter. Starting a journal involves some
  *   effectful steps, such as creating Cassandra session, so, in case of
  *   infrastructure or configuration troubles, it could take a longer time.
  *   Creating the journal will fail with [[TimeoutException]] if it takes
  *   longer than `startTimeout`.
  * @param stopTimeout
  *   This is meant to be a counterpart to `startTimeout`, allowing resource
  *   release to timeout with an error. This parameter is not used, for now, and
  *   `startTimeout` is used instead.
  * @param maxEventsInBatch
  *   Maximum number of events to write in one batch. I.e. if there is a request
  *   to write N > `maxEventsInBatch` events then the batch will be split to the
  *   writes of no more than `maxEventsInBatch` improving a latency and reducing
  *   the effect of potential failures.
  * @param callTimeThresholds
  *   Duration after a call to one of [[Journal]] methods will be reported as
  *   warning if it takes longer than the specified value. The calls to these
  *   methods are logged anyway, but on a debug logger level, which is, usually,
  *   switched off on production deployments.
  * @param jsonCodec
  *   JSON codec to use for (de)serialization of the events from
  *   [[scodec.bits.ByteVector]] to [[play.api.libs.json.JsValue]] and
  *   vice-versa. This parameter is only relevant if default [[KafkaJournal]] is
  *   used, i.e. it is not taken into account if Circe JSON or other custom
  *   serialization is used.
  * @param consumerPoolSize TODO: write doc after MR is approved
  *
  * @see [[KafkaJournal]] for more details.
  */
final case class KafkaJournalConfig(
  journal: JournalConfig = JournalConfig.default,
  cassandra: EventualCassandraConfig = EventualCassandraConfig.default,
  startTimeout: FiniteDuration = 1.minute,
  stopTimeout: FiniteDuration = 1.minute,
  maxEventsInBatch: Int = 100,
  callTimeThresholds: CallTimeThresholds = CallTimeThresholds.default,
  jsonCodec: KafkaJournalConfig.JsonCodec = KafkaJournalConfig.JsonCodec.Default,
  consumerPool: Option[ConsumerPoolConfig] = None,
)

object KafkaJournalConfig {

  val default: KafkaJournalConfig = KafkaJournalConfig()

  implicit val configReaderKafkaJournalConfig: ConfigReader[KafkaJournalConfig] = {

    val configReader = deriveReader[KafkaJournalConfig]

    cursor: ConfigCursor => {
      for {
        cursor  <- cursor.asObjectCursor
        config   = cursor.objValue.toConfig
        source   = ConfigSource.fromConfig(config)
        config  <- source.load(configReader)
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
