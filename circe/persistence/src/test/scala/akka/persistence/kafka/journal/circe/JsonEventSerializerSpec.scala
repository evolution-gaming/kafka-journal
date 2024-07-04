package akka.persistence.kafka.journal.circe

import akka.persistence.PersistentRepr
import akka.persistence.journal.Tagged
import akka.persistence.kafka.journal.EventSerializer
import cats.effect.IO
import cats.syntax.all._
import com.evolutiongaming.kafka.journal.IOSuite._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.circe._
import com.evolutiongaming.kafka.journal.util.ScodecHelper._
import io.circe.Json
import io.circe.jawn._
import org.scalatest.EitherValues
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers
import play.api.libs.json._

class JsonEventSerializerSpec extends AsyncFunSuite with ActorSuite with Matchers with EitherValues {

  private val serializer = KafkaJournalCirce.JsonEventSerializer.of[IO]

  for {
    (name, payload) <- List(
      ("json", Json.fromString("json")),
      ("text", "text"),
    )
  } {
    test(s"toEvent & toPersistentRepr: $name") {
      val persistentR = persistentRepr(payload)
      val check = for {
        event  <- serializer.toEvent(persistentR)
        actual <- serializer.toPersistentRepr(persistentR.persistenceId, event)
      } yield {
        actual shouldBe persistentR
      }

      check.run()
    }
  }

  for {
    (fileName, payload) <- List(
      ("PersistentRepr.json", Json.fromString("json")),
      ("PersistentRepr.text.json", "text"),
    )
  } {
    test(s"toEvent: $fileName") {
      val check = for {
        expectedJson <- readJsonFromFile(fileName)
        persistentRepr = PersistentRepr(
          payload       = payload,
          sequenceNr    = 1,
          persistenceId = "persistenceId",
          manifest      = "manifest",
          writerUuid    = "writerUuid",
        )
        actual <- serializer.toEvent(persistentRepr)
      } yield {
        actual.payload shouldBe expectedJson.some
      }

      check.run()
    }
  }

  test("toEvent: tags") {
    val tags = Set("1", "2")

    val check = for {
      actual <- serializer.toEvent(persistentRepr(Tagged(Json.fromString("json"), tags)))
    } yield {
      actual.tags shouldBe tags
    }

    check.run()
  }

  for {
    (name, circePayload, playPayload) <- List(
      ("json", Json.fromString("json"), JsString("json")),
      ("text", "text", "text"),
    )
  } {

    val persistentReprCirce = persistentRepr(circePayload)
    val persistentReprPlay  = persistentRepr(playPayload)

    test(s"toEvent with Circe & toPersistentRepr with Play: $name") {
      val check = for {
        playJsonSerializer <- EventSerializer.of[IO](actorSystem)

        circeJsonEvent <- serializer.toEvent(persistentReprCirce)
        playJsonEvent  <- circeJsonEvent.traverse(json => circeToPlay(json).map(Payload.json(_)))

        actual <- playJsonSerializer.toPersistentRepr(persistentReprCirce.persistenceId, playJsonEvent)
      } yield {
        actual shouldBe persistentReprPlay
      }

      check.run()
    }

    test(s"toEvent with Play & toPersistentRepr with Circe: $name") {
      val check = for {
        playJsonSerializer <- EventSerializer.of[IO](actorSystem)

        playJsonEvent <- playJsonSerializer.toEvent(persistentReprPlay)
        circeJsonEvent = playJsonEvent.map {
          case p: Payload.Json => convertPlayToCirce(p.value)
          case _               => fail("json is expected after serialization")
        }

        actual <- serializer.toPersistentRepr(persistentReprPlay.persistenceId, circeJsonEvent)
      } yield {
        actual shouldBe persistentReprCirce
      }

      check.run()
    }
  }

  test("toEvent: unsupported payload") {
    val persistenceId      = "12345"
    val unsupportedPayload = 42

    val serializer = KafkaJournalCirce.JsonEventSerializer.of[Either[Throwable, *]]
    val result     = serializer.toEvent(persistentRepr(unsupportedPayload, persistenceId))

    result.left.value shouldBe a[JournalError]
    result.left.value.getMessage should include(s"persistenceId: $persistenceId")
    result.left.value.getCause shouldBe a[JournalError]
    result.left.value.getCause.getMessage shouldBe s"Event.payload is not supported, payload: $unsupportedPayload"
  }

  private def circeToPlay(json: Json): IO[JsValue] =
    convertCirceToPlay(json)
      .leftMap(new RuntimeException(_))
      .liftTo[IO]

  private def persistentRepr(payload: Any, persistenceId: String = "persistenceId") =
    PersistentRepr(
      payload       = payload,
      sequenceNr    = 1,
      persistenceId = persistenceId,
      manifest      = "manifest",
      writerUuid    = "writerUuid",
    )

  private def readJsonFromFile(name: String): IO[Json] =
    for {
      bytes <- ByteVectorOf[IO](EventSerializer.getClass, name)
      str   <- bytes.decodeStr.liftTo[IO]
      json  <- FromCirceResult.summon[IO].apply(parse(str))
    } yield json

}
