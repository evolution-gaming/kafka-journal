package akka.persistence.kafka.journal

import java.io.FileOutputStream

import akka.persistence.PersistentRepr
import akka.persistence.serialization.Snapshot
import cats.effect.{IO, Sync}
import com.evolutiongaming.kafka.journal.FromBytes.implicits._
import com.evolutiongaming.kafka.journal.IOSuite._
import com.evolutiongaming.kafka.journal._
import org.scalatest.{AsyncFunSuite, Matchers}
import play.api.libs.json.{JsString, JsValue}
import scodec.bits.ByteVector

import scala.util.Try

class EventSerializerSpec extends AsyncFunSuite with ActorSuite with Matchers {

  for {
    (name, payloadType, payload) <- List(
      ("PersistentRepr.bin",       PayloadType.Binary, Snapshot("binary")),
      ("PersistentRepr.text.json", PayloadType.Json, "text"),
      ("PersistentRepr.json",      PayloadType.Json, JsString("json")))
  } {

    test(s"toEvent & toPersistentRepr, payload: $payload") {
      val persistenceId = "persistenceId"
      val persistentRepr = PersistentRepr(
        payload = payload,
        sequenceNr = 1,
        persistenceId = persistenceId,
        manifest = "manifest",
        writerUuid = "writerUuid")

      val fa = for {
        serializer <- EventSerializer.of[IO](actorSystem)
        event      <- serializer.toEvent(persistentRepr)
        actual     <- serializer.toPersistentRepr(persistenceId, event)
        _          <- Sync[IO].delay { actual shouldEqual persistentRepr }
        payload1   <- Sync[IO].catchNonFatal { event.payload getOrElse sys.error("Event.payload is not defined") }
        _          <- Sync[IO].delay { payload1.payloadType shouldEqual payloadType }
        bytes      <- Sync[IO].delay { ByteVectorOf(getClass, name) }
      } yield {
        /*persistentPayload match {
        case a: Payload.Binary => writeToFile(a.value, name)
        case _: Payload.Text   =>
        case _: Payload.Json   =>
      }*/

        payload1 match {
          case payload: Payload.Binary => payload.value shouldEqual bytes
          case payload: Payload.Text   => payload.value shouldEqual bytes.fromBytes[Try, String].get
          case payload: Payload.Json   => payload.value shouldEqual bytes.fromBytes[Try, JsValue].get
        }
      }

      fa.run()
    }
  }

  def writeToFile[F[_] : Sync](bytes: ByteVector, path: String): F[Unit] = {
    Sync[F].delay {
      val os = new FileOutputStream(path)
      os.write(bytes.toArray)
      os.close()
    }
  }
}
