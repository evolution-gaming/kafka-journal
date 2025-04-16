package akka.persistence.kafka.journal

import akka.persistence.PersistentRepr
import akka.persistence.serialization.Snapshot
import cats.effect.IO
import com.evolutiongaming.kafka.journal.*
import com.evolutiongaming.kafka.journal.IOSuite.*
import com.evolutiongaming.kafka.journal.util.CatsHelper.*
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers
import play.api.libs.json.JsString

class EventSerializerSpec extends AsyncFunSuite with ActorSuite with Matchers with SerdeTesting {

  for {
    (name, payloadType, payload) <- List(
      ("PersistentRepr.bin", PayloadType.Binary, Snapshot("binary")),
      ("PersistentRepr.text.json", PayloadType.Json, "text"),
      ("PersistentRepr.json", PayloadType.Json, JsString("json")),
    )
  } {

    test(s"toEvent & toPersistentRepr, payload: $payload") {
      val persistenceId = "persistenceId"
      val persistentRepr = PersistentRepr(
        payload = payload,
        sequenceNr = 1,
        persistenceId = persistenceId,
        manifest = "manifest",
        writerUuid = "writerUuid",
      )

      val fa = for {
        serializer <- EventSerializer.of[IO](actorSystem)
        event <- serializer.toEvent(persistentRepr)
        actual <- serializer.toPersistentRepr(persistenceId, event)
        _ <- IO { actual shouldEqual persistentRepr }
        payload <- event.payload.getOrError[IO]("Event.payload is not defined")
        _ = payload.payloadType shouldEqual payloadType
//        _ <- payload match {
//          case a: Payload.Binary => IO { dumpEncodedDataToFile(a.value, name) }
//          case _: Payload.Text => IO.unit
//          case _: Payload.Json => IO.unit
//        }
        payloadExample <- IO { readSerdeExampleFileAsPayload(name, payloadType) }
      } yield {
        payload shouldEqual payloadExample
      }

      fa.run()
    }
  }
}
