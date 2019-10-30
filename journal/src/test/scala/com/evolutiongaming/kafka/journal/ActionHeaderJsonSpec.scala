package com.evolutiongaming.kafka.journal

import cats.implicits._
import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json._

import scala.concurrent.duration._


class ActionHeaderJsonSpec extends FunSuite with Matchers {

  val origins = List(Some(Origin("origin")), None)
  val metadata = List(
    ("metadata", RecordMetadata(data = Some(Json.obj(("key", "value"))))),
    ("none"    , RecordMetadata(data = None)),
    ("legacy"  , RecordMetadata(data = None)))
  val payloadTypes = List(PayloadType.Binary, PayloadType.Json)

  for {
    origin <- origins
  } {
    val originStr = origin.fold("None")(_.toString)
    val expireAfter = origin.fold { 1.day.some } { _ => none[FiniteDuration] }
    for {
      payloadType             <- payloadTypes
      (metadataStr, metadata) <- metadata
    } {
      test(s"Append format, origin: $origin, payloadType: $payloadType, metadata: $metadataStr") {
        val range = SeqRange.unsafe(1, 5)
        val header = ActionHeader.Append(
          range = range,
          origin = origin,
          payloadType = payloadType,
          metadata = metadata,
          expireAfter = expireAfter)
        verify(header, s"Append-$originStr-$payloadType-$metadataStr")
      }
    }

    test(s"Delete format, origin: $origin") {
      val seqNr = SeqNr.unsafe(3)
      val header = ActionHeader.Delete(seqNr, origin)
      verify(header, s"Delete-$originStr")
    }

    test(s"Mark format, origin: $origin, ") {
      val header = ActionHeader.Mark("id", origin)
      verify(header, s"Mark-$originStr")
    }
  }

  private def verify(value: ActionHeader, name: String) = {

    def verify(json: JsValue) = {
      val actual = json.validate[ActionHeader]
      actual shouldEqual JsSuccess(value)
    }

    verify(Json.toJson(value))
    verify(Json.parse(ByteVectorOf(getClass, s"$name.json").toArray))
  }
}
