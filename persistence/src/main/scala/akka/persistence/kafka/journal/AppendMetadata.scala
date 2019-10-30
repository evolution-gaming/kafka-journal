package akka.persistence.kafka.journal

import com.evolutiongaming.kafka.journal.Headers
import play.api.libs.json.JsValue

import scala.concurrent.duration.FiniteDuration

final case class AppendMetadata(
  expireAfter: Option[FiniteDuration] = None,
  metadata: Option[JsValue] = None,
  headers: Headers = Headers.empty)

object AppendMetadata {
  val empty: AppendMetadata = AppendMetadata()
}
