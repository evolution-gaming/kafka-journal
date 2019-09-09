package akka.persistence.kafka.journal

import com.evolutiongaming.kafka.journal.PayloadType
import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.{JsSuccess, Json}

class PersistentJsonSpec extends FunSuite with Matchers {

  for {
    payloadType <- List(Some(PayloadType.Json), Some(PayloadType.Text), None)
    manifest    <- List(None, Some("manifest"), Some(""))
  } {
    test(s"toJson & fromJson, payloadType: $payloadType, manifest: $manifest") {
      val persistent = PersistentJson(
        manifest = manifest,
        writerUuid = "writerUuid",
        payloadType = payloadType,
        payload = "payload")
      val json = Json.toJson(persistent)
      json.validate[PersistentJson[String]] shouldEqual JsSuccess(persistent)
    }
  }
}
