package com.evolution.kafka.journal

import cats.syntax.all.*
import com.evolution.kafka.journal.util.PlayJsonHelper.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import play.api.libs.json.*

import scala.util.Try

class ActionHeaderJsonSpec extends AnyFunSuite with Matchers with SerdeTesting {

  // For some reason, ActionHeader Format is asymmetric:
  // it has Writes for ActionHeader but Reads only for Option[ActionHeader].
  // For this test, we can define Reads[ActionHeader] through the Option impl.
  private implicit val actionHeaderReads: Reads[ActionHeader] = (json: JsValue) => {
    ActionHeader.formatOptActionHeader.reads(json).map(_.get)
  }
  private implicit val actionHeaderFromBytes: FromBytes[Try, ActionHeader] = FromBytes.fromReads[Try, ActionHeader]

  private val originExamples = Vector(Origin("origin").some, none)
  private val metadataExamplesCurrentFormat = Vector(
    ("metadata", HeaderMetadata(Json.obj(("key", "value")).some)),
    ("none", HeaderMetadata.empty),
  )
  private val metadataExamplesLegacyFormat = Vector(
    ("legacy", HeaderMetadata.empty),
  )
  private val payloadTypeExamples = Vector(PayloadType.Binary, PayloadType.Json)

  private def appendExample(
    origin: Option[Origin],
    payloadType: PayloadType.BinaryOrJson,
    metadata: HeaderMetadata,
  ): ActionHeader = {
    ActionHeader.Append(
      range = SeqRange.unsafe(1, 5),
      origin = origin,
      version = none,
      payloadType = payloadType,
      metadata = metadata,
    )
  }

  originExamples.foreach { origin =>
    val originStr = origin.fold("None")(_.toString)

    payloadTypeExamples.foreach { payloadType =>
      metadataExamplesCurrentFormat.foreach { case (metadataStr, metadata) =>
        test(s"Append current format, origin: $origin, payloadType: $payloadType, metadata: $metadataStr") {
          val header = appendExample(origin, payloadType, metadata)
          verifyEncodeDecodeExample(header, s"Append-$originStr-$payloadType-$metadataStr.json")
        }
      }

      metadataExamplesLegacyFormat.foreach { case (metadataStr, metadata) =>
        test(s"Append legacy format, origin: $origin, payloadType: $payloadType, metadata: $metadataStr") {
          val header = appendExample(origin, payloadType, metadata)
          verifyDecodeExample(header, s"Append-$originStr-$payloadType-$metadataStr.json")
        }
      }
    }
  }

  originExamples.foreach { origin =>
    val originStr = origin.fold("None")(_.toString)

    test(s"Delete current format, origin: $origin") {
      val seqNr = SeqNr.unsafe(3)
      val header = ActionHeader.Delete(seqNr.toDeleteTo, origin, Version("0.0.1").some)
      verifyEncodeDecodeExample[ActionHeader](header, s"Delete-$originStr.json")
    }

    test(s"Purge current format, origin: $origin") {
      val header = ActionHeader.Purge(origin, none)
      verifyEncodeDecodeExample[ActionHeader](header, s"Purge-$originStr.json")
    }

    test(s"Mark current format, origin: $origin") {
      val header = ActionHeader.Mark("id", origin, none)
      verifyEncodeDecodeExample[ActionHeader](header, s"Mark-$originStr.json")
    }
  }

  test("not supported ActionHeader") {
    val json = Json.obj(("new", Json.obj()))
    json.validate[Option[ActionHeader]] shouldEqual none[ActionHeader].pure[JsResult]
  }
}
