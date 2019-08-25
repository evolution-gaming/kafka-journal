package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.SeqNr.implicits._
import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json._

class ActionHeaderJsonSpec extends FunSuite with Matchers {

  val origins = List(Some(Origin("origin")), None)
  val metadata = List(
    ("metadata", Metadata(data = Some(Json.obj(("key", "value"))))),
    ("none"    , Metadata(data = None)),
    ("legacy"  , Metadata(data = None)))
  val payloadTypes = List(PayloadType.Binary, PayloadType.Json)

  for {
    origin <- origins
  } {
    val originStr = origin.fold("None")(_.toString)
    for {
      payloadType             <- payloadTypes
      (metadataStr, metadata) <- metadata
    } {
      test(s"Append format, origin: $origin, payloadType: $payloadType, metadata: $metadataStr") {
        val range = SeqRange(1, 5)
        val header = ActionHeader.Append(range, origin, payloadType, metadata)
        verify(header, s"Append-$originStr-$payloadType-$metadataStr")
      }
    }

    test(s"Delete format, origin: $origin") {
      val seqNr = 3.toSeqNr
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
