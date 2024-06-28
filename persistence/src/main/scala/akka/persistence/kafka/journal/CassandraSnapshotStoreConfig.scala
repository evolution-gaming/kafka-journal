package akka.persistence.kafka.journal

import com.evolutiongaming.kafka.journal.snapshot.cassandra.SnapshotCassandraConfig
import pureconfig.generic.semiauto.deriveReader
import pureconfig.{ConfigCursor, ConfigReader, ConfigSource}

import scala.concurrent.duration._

/** Configuration for [[CassandraSnapshotStore]].
  *
  * This case class specifies configuration that could be set using `application.conf` (see `reference.conf` for an
  * example of such configuration).
  *
  * @param cassandra
  *   Cassandra-specific configuration used by a plugin.
  * @param startTimeout
  *   The timeout to create a journal adapter. Starting a journal involves some effectful steps, such as creating
  *   Cassandra session, so, in case of infrastructure or configuration troubles, it could take a longer time. Creating
  *   the journal will fail with [[TimeoutException]] if it takes longer than `startTimeout`.
  * @param stopTimeout
  *   This is meant to be a counterpart to `startTimeout`, allowing resource release to timeout with an error. This
  *   parameter is not used, for now, and `startTimeout` is used instead.
  * @param jsonCodec
  *   JSON codec to use for (de)serialization of the events from [[scodec.bits.ByteVector]] to
  *   [[play.api.libs.json.JsValue]] and vice-versa. This parameter is only relevant if default [[CassandraSnapshotStore]] is
  *   used, i.e. it is not taken into account if Circe JSON or other custom serialization is used.
  *
  * @see
  *   [[KafkaJournal]] for more details.
  */
final case class CassandraSnapshotStoreConfig(
  cassandra: SnapshotCassandraConfig = SnapshotCassandraConfig.default,
  startTimeout: FiniteDuration = 1.minute,
  stopTimeout: FiniteDuration = 1.minute,
  jsonCodec: KafkaJournalConfig.JsonCodec = KafkaJournalConfig.JsonCodec.Default
)

object CassandraSnapshotStoreConfig {

  val default: CassandraSnapshotStoreConfig = CassandraSnapshotStoreConfig()

  implicit val configReaderKafkaJournalConfig: ConfigReader[CassandraSnapshotStoreConfig] = {

    val configReader = deriveReader[CassandraSnapshotStoreConfig]

    cursor: ConfigCursor => {
      for {
        cursor <- cursor.asObjectCursor
        config = cursor.objValue.toConfig
        source = ConfigSource.fromConfig(config)
        config <- source.load(configReader)
      } yield config
    }
  }

}
