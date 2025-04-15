package com.evolutiongaming.kafka.journal

import cats.data.NonEmptyList as Nel
import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.ExpireAfter.implicits.*
import com.evolutiongaming.kafka.journal.SerdeTesting.EncodedDataType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import play.api.libs.json.Json

import scala.concurrent.duration.*

class KafkaReadWriteFormatSpec extends AnyFunSuite with Matchers with SerdeTesting with KafkaReadWriteTesting {

  private val payloadMetadata = PayloadMetadata(1.day.toExpireAfter.some, Json.obj(("key", "value")).some)

  for {
    (name, payloadType, events) <- List(
      ("empty", PayloadType.Json, Events(Nel.of(event(1)), PayloadMetadata.empty)),
      ("binary", PayloadType.Binary, Events(Nel.of(event(1, binary("payload"))), PayloadMetadata.empty)),
      (
        "text",
        PayloadType.Json,
        Events(Nel.of(event(1, Payload.text(""" {"key":"value"} """))), PayloadMetadata.empty),
      ),
      ("json", PayloadType.Json, Events(Nel.of(event(1, Payload.json("payload"))), PayloadMetadata.empty)),
      ("empty-many", PayloadType.Json, Events(Nel.of(event(1), event(2)), payloadMetadata)),
      (
        "binary-many",
        PayloadType.Binary,
        Events(Nel.of(event(1, binary("1")), event(2, binary("2"))), payloadMetadata),
      ),
      (
        "text-many",
        PayloadType.Json,
        Events(Nel.of(event(1, Payload.text("1")), event(2, Payload.text("2"))), payloadMetadata),
      ),
      (
        "json-many",
        PayloadType.Json,
        Events(Nel.of(event(1, Payload.json("1")), event(2, Payload.json("2"))), payloadMetadata),
      ),
      (
        "empty-binary-text-json",
        PayloadType.Binary,
        Events(
          Nel.of(event(1), event(2, binary("binary")), event(3, Payload.text("text")), event(4, Payload.json("json"))),
          payloadMetadata,
        ),
      ),
    )
  } {

    test(s"toBytes & fromBytes current format, events: $name") {
      val exampleFileName = s"Payload-$name.${ EncodedDataType.fromPayloadType(payloadType).fileExtension }"
      verifyKafkaReadWriteExample(
        valueExample = events,
        encodedExampleFileName = exampleFileName,
        examplePayloadType = payloadType,
//        dumpEncoded = true,
      )
    }
  }

  for {
    (name, payloadType, events) <- List(
      ("empty", PayloadType.Json, Events(Nel.of(event(1)), PayloadMetadata.empty)),
      (
        "text",
        PayloadType.Json,
        Events(Nel.of(event(1, Payload.text(""" {"key":"value"} """))), PayloadMetadata.empty),
      ),
      ("json", PayloadType.Json, Events(Nel.of(event(1, Payload.json("payload"))), PayloadMetadata.empty)),
      ("empty-many", PayloadType.Json, Events(Nel.of(event(1), event(2)), PayloadMetadata.empty)),
      (
        "text-many",
        PayloadType.Json,
        Events(Nel.of(event(1, Payload.text("1")), event(2, Payload.text("2"))), PayloadMetadata.empty),
      ),
      (
        "json-many",
        PayloadType.Json,
        Events(Nel.of(event(1, Payload.json("1")), event(2, Payload.json("2"))), PayloadMetadata.empty),
      ),
    )
  } {

    test(s"fromBytes v0 format, events: $name") {
      val exampleFileName = s"Payload-v0-$name.${ payloadType.ext }"
      verifyKafkaReadExample(
        valueExample = events,
        examplePayloadType = payloadType,
        encodedExampleFileName = exampleFileName,
      )
    }
  }

  private def event(seqNr: Int, payload: Option[Payload] = None): Event[Payload] = {
    val tags = (0 to seqNr).map(_.toString).toSet
    Event(SeqNr.unsafe(seqNr), tags, payload)
  }

  private def event(seqNr: Int, payload: Payload): Event[Payload] = event(seqNr, payload.some)

  private def binary(a: String): Payload.Binary = PayloadBinaryFromStr(a)
}
