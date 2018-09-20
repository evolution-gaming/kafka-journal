package akka.persistence.kafka.journal

import com.evolutiongaming.kafka.journal.PayloadType
import play.api.libs.json.{JsValue, Json, OFormat}

final case class PersistentJson(
  manifest: String,
  writerUuid: String,
  payloadType: Option[PayloadType.TextOrJson],
  payload: JsValue)


object PersistentJson {
  implicit val Format: OFormat[PersistentJson] = Json.format[PersistentJson]
}
