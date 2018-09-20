package akka.persistence.kafka.journal

import com.evolutiongaming.kafka.journal.PayloadType
import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.{JsString, Json}

class PersistentJsonSpec extends FunSuite with Matchers {

  for {
    payloadType <- List(
      Some(PayloadType.Json),
      Some(PayloadType.Text),
      None)
  } {
    test(s"toJson & fromJson, payloadType: $payloadType") {
      val persistent = PersistentJson(
        manifest = "manifest",
        writerUuid = "writerUuid",
        payloadType = payloadType,
        payload = JsString("payload"))
      val json = Json.toJson(persistent)
      json.as[PersistentJson] shouldEqual persistent
    }
  }
}
