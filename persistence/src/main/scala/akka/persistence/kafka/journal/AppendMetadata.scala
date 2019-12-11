package akka.persistence.kafka.journal

import com.evolutiongaming.kafka.journal.{ExpireAfter, Headers}
import play.api.libs.json.JsValue

final case class AppendMetadata(
  expireAfter: Option[ExpireAfter] = None,
  metadata: Option[JsValue] = None,
  headers: Headers = Headers.empty)

object AppendMetadata {
  val empty: AppendMetadata = AppendMetadata()
}
