package akka.persistence.kafka.journal

import com.evolutiongaming.kafka.journal.PayloadType
import play.api.libs.json.{Format, Json, OFormat}

final case class PersistentJson[A](
  manifest: Option[String],
  writerUuid: String,
  payloadType: Option[PayloadType.TextOrJson],
  payload: A)


object PersistentJson {

  implicit def formatPersistentJson[A: Format]: OFormat[PersistentJson[A]] = Json.format
}
