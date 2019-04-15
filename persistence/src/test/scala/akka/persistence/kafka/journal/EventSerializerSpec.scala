package akka.persistence.kafka.journal

import java.io.FileOutputStream

import akka.persistence.PersistentRepr
import akka.persistence.serialization.Snapshot
import com.evolutiongaming.kafka.journal.FixEquality.Implicits._
import com.evolutiongaming.kafka.journal.FromBytes.Implicits._
import com.evolutiongaming.kafka.journal._
import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.{JsString, JsValue}

class EventSerializerSpec extends FunSuite with ActorSuite with Matchers {

  private implicit val fixEquality = FixEquality.array[Byte]()

  for {
    (name, payloadType, payload) <- List(
      ("PersistentRepr.bin", PayloadType.Binary, Snapshot("binary")),
      ("PersistentRepr.text.json", PayloadType.Json, "text"),
      ("PersistentRepr.json", PayloadType.Json, JsString("json")))
  } {

    test(s"toEvent & toPersistentRepr, payload: $payload") {
      val serializer = EventSerializer.unsafe(system)
      val persistenceId = "persistenceId"
      val persistentRepr = PersistentRepr(
        payload = payload,
        sequenceNr = 1,
        persistenceId = persistenceId,
        manifest = "manifest",
        writerUuid = "writerUuid")
      val event = serializer.toEvent(persistentRepr)
      serializer.toPersistentRepr(persistenceId, event) shouldEqual persistentRepr

      val persistentPayload = event.payload getOrElse sys.error("Event.payload is not defined")
      persistentPayload.payloadType shouldEqual payloadType

      /*persistentPayload match {
        case a: Payload.Binary => writeToFile(a.value, name)
        case _: Payload.Text   =>
        case _: Payload.Json   =>
      }*/

      val bytes = BytesOf(getClass, name)
      persistentPayload match {
        case payload: Payload.Binary => payload.value.fix shouldEqual bytes.fix
        case payload: Payload.Text   => payload.value shouldEqual bytes.fromBytes[String]
        case payload: Payload.Json   => payload.value shouldEqual bytes.fromBytes[JsValue]
      }
    }
  }

  def writeToFile(bytes: Bytes, path: String): Unit = {
    val os = new FileOutputStream(path)
    os.write(bytes)
    os.close()
  }
}
