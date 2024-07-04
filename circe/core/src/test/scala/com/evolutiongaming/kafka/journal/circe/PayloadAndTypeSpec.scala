package com.evolutiongaming.kafka.journal.circe

import cats.data.NonEmptyList as Nel
import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.*
import com.evolutiongaming.kafka.journal.TestJsonCodec.instance
import com.evolutiongaming.kafka.journal.circe.Instances.*
import com.evolutiongaming.kafka.journal.conversions.*
import io.circe.Json as CirceJson
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import play.api.libs.json.Json as PlayJson
import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets
import scala.concurrent.duration.*
import scala.util.Try

class PayloadAndTypeSpec extends AnyFunSuite with Matchers with EitherValues {

  private implicit val fromAttempt: FromAttempt[Try]   = FromAttempt.lift[Try]
  private implicit val fromJsResult: FromJsResult[Try] = FromJsResult.lift[Try]

  private val playKafkaWrite = KafkaWrite.summon[Try, Payload]
  private val playKafkaRead  = KafkaRead.summon[Try, Payload]

  private val circeKafkaRead  = KafkaRead.summon[Try, CirceJson]
  private val circeKafkaWrite = KafkaWrite.summon[Try, CirceJson]

  private val payloadMetadata = PayloadMetadata(
    ExpireAfter(1.day).some,
    PlayJson.obj(("key", "value")).some,
  )

  for {
    (metadataName, metadata) <- List(
      ("with metadata", payloadMetadata),
      ("empty", PayloadMetadata.empty),
    )
    (eventsName, events) <- List(
      ("empty", Events(Nel.of(event[CirceJson](1)), metadata)),
      ("empty-many", Events(Nel.of(event[CirceJson](1), event[CirceJson](2)), metadata)),
      ("json", Events(Nel.of(event(1, CirceJson.fromString("payload"))), metadata)),
      (
        "json-many",
        Events(Nel.of(event(1, CirceJson.fromString("payload1")), event(2, CirceJson.fromString("payload2"))), metadata),
      ),
    )
  } {
    test(s"toBytes & fromBytes, events: $eventsName, metadata: $metadataName") {
      val actual = for {
        payloadAndType <- circeKafkaWrite(events)
        _               = payloadAndType.payloadType shouldBe PayloadType.Json
        actual         <- circeKafkaRead(payloadAndType)
      } yield actual

      actual shouldBe events.pure[Try]
    }
  }

  for {
    (metadataName, metadata) <- List(
      ("with metadata", payloadMetadata),
      ("empty", PayloadMetadata.empty),
    )
    (eventsName, eventsPlayJson, eventsCirceJson) <- List(
      ("empty", Events(Nel.of(event[Payload](1)), metadata), Events(Nel.of(event[CirceJson](1)), metadata)),
      (
        "empty-many",
        Events(Nel.of(event(1, none[Payload]), event(2, none[Payload])), metadata),
        Events(Nel.of(event(1, none[CirceJson]), event(2, none[CirceJson])), metadata),
      ),
      (
        "json",
        Events(Nel.of(event(1, Payload.json(PlayJson.obj("key" -> "1")))), metadata),
        Events(Nel.of(event(1, CirceJson.obj("key" -> CirceJson.fromString("1")))), metadata),
      ),
      (
        "json-many",
        Events(
          Nel.of(event(1, Payload.json(PlayJson.obj("key" -> "1"))), event(2, Payload.json(PlayJson.obj("key" -> "2")))),
          metadata,
        ),
        Events(
          Nel.of(
            event(1, CirceJson.obj("key" -> CirceJson.fromString("1"))),
            event(2, CirceJson.obj("key" -> CirceJson.fromString("2"))),
          ),
          metadata,
        ),
      ),
    )
  } {
    test(s"toBytes with Play, fromBytes with Circe: $eventsName, metadata: $metadataName") {
      val actual = for {
        payloadAndType <- playKafkaWrite(eventsPlayJson)
        actual         <- circeKafkaRead(payloadAndType)
      } yield actual

      actual shouldBe eventsCirceJson.pure[Try]
    }

    test(s"toBytes with Circe, fromBytes with Play: $eventsName, metadata: $metadataName") {
      val actual = for {
        payloadAndType <- circeKafkaWrite(eventsCirceJson)
        actual         <- playKafkaRead(payloadAndType)
      } yield actual

      actual shouldBe eventsPlayJson.pure[Try]
    }
  }

  for {
    (name, events) <- List(
      ("empty", Events(Nel.of(event[CirceJson](1)), PayloadMetadata.empty)),
      ("text", Events(Nel.of(event(1, CirceJson.fromString(""" {"key":"value"} """))), PayloadMetadata.empty)),
      ("json", Events(Nel.of(event(1, CirceJson.fromString("payload"))), PayloadMetadata.empty)),
      ("empty-many", Events(Nel.of(event[CirceJson](1), event[CirceJson](2)), PayloadMetadata.empty)),
      (
        "text-many",
        Events(Nel.of(event(1, CirceJson.fromString("1")), event(2, CirceJson.fromString("2"))), PayloadMetadata.empty),
      ),
      (
        "json-many",
        Events(Nel.of(event(1, CirceJson.fromString("1")), event(2, CirceJson.fromString("2"))), PayloadMetadata.empty),
      ),
    )
  } {
    test(s"fromBytes, events: $name") {
      val ext = PayloadType.Json.ext
      val actual = for {
        payload       <- ByteVectorOf[Try](PayloadAndType.getClass, s"Payload-v0-$name.$ext")
        payloadAndType = PayloadAndType(payload, PayloadType.Json)
        events        <- circeKafkaRead(payloadAndType)
      } yield events
      actual shouldEqual events.pure[Try]
    }
  }

  test("fromBytes: returns an error for non-json payload") {
    val payloadAndType = PayloadAndType(ByteVector.empty, PayloadType.Binary)

    val result = circeKafkaRead(payloadAndType).toEither

    result.left.value shouldBe a[JournalError]
    result.left.value.getMessage should include("Binary")
  }

  test("fromBytes: returns an error for malformed json") {
    val malformed      = ByteVector.view("{\"key\": {sss}}".getBytes(StandardCharsets.UTF_8))
    val payloadAndType = PayloadAndType(malformed, PayloadType.Json)

    val result = circeKafkaRead(payloadAndType).toEither

    result.left.value shouldBe a[JournalError]
    result.left.value.getMessage should (include("ParsingFailure") and include("sss"))
  }

  def event[A](seqNr: Int, payload: Option[A] = None): Event[A] = {
    val tags = (0 to seqNr).map(_.toString).toSet
    Event(SeqNr.unsafe(seqNr), tags, payload)
  }

  def event[A](seqNr: Int, payload: A): Event[A] = {
    event(seqNr, payload.some)
  }

}
