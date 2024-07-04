package akka.persistence.kafka.journal

import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.PayloadType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import play.api.libs.json.{JsSuccess, Json}

class PersistentJsonSpec extends AnyFunSuite with Matchers {

  for {
    payloadType <- List(PayloadType.Json.some, PayloadType.Text.some, none)
    manifest    <- List(none, "manifest".some, "".some)
  } {
    test(s"toJson & fromJson, payloadType: $payloadType, manifest: $manifest") {
      val persistent =
        PersistentJson(manifest = manifest, writerUuid = "writerUuid", payloadType = payloadType, payload = "payload")
      val json = Json.toJson(persistent)
      json.validate[PersistentJson[String]] shouldEqual JsSuccess(persistent)
    }
  }
}
