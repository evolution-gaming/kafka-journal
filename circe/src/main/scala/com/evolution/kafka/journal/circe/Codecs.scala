package com.evolution.kafka.journal.circe

import cats.syntax.all.*
import com.evolution.kafka.journal.*
import com.evolution.kafka.journal.PayloadAndType.*
import com.evolution.kafka.journal.PayloadType.TextOrJson
import io.circe.*
import io.circe.generic.semiauto.*
import play.api.libs.json.JsValue

import scala.concurrent.duration.*

object Codecs {

  implicit val finiteDurationEncoder: Encoder[FiniteDuration] = Encoder.encodeString.contramap(_.toString)
  implicit val finiteDurationDecoder: Decoder[FiniteDuration] = Decoder.decodeString.emap { str =>
    Either
      .catchNonFatal(Duration(str).asInstanceOf[FiniteDuration])
      .leftMap(_ => s"cannot parse FiniteDuration from $str")
  }

  implicit val expireAfterEncoder: Encoder[ExpireAfter] = finiteDurationEncoder.contramap(_.value)
  implicit val expireAfterDecoder: Decoder[ExpireAfter] = finiteDurationDecoder.map(ExpireAfter(_))

  implicit val jsValueEncoder: Encoder[JsValue] = Encoder.instance(convertPlayToCirce)
  implicit val jsValueDecoder: Decoder[JsValue] = Decoder.decodeJson.emap(convertCirceToPlay)

  implicit val payloadMetadataCodec: Codec[PayloadMetadata] = deriveCodec

  implicit val seqNrEncoder: Encoder[SeqNr] = Encoder.encodeLong.contramap(_.value)
  implicit val seqNrDecoder: Decoder[SeqNr] = Decoder.decodeLong.emap(SeqNr.of[Either[String, *]](_))

  implicit val payloadTypeEncoder: Encoder[PayloadType.TextOrJson] = Encoder.encodeString.contramap(_.name)
  implicit val payloadTypeDecoder: Decoder[PayloadType.TextOrJson] = Decoder.decodeString.emap { str =>
    PayloadType(str)
      .flatMap {
        case v: TextOrJson => v.some
        case _ => none
      }
      .toRight(s"No PayloadType.TextOrJson found by $str")
  }

  implicit def eventJsonEncoder[A: Encoder]: Encoder[EventJson[A]] = deriveEncoder
  implicit def eventJsonDecoder[A: Decoder]: Decoder[EventJson[A]] = deriveDecoder

  implicit def payloadJsonEncoder[A: Encoder]: Encoder[PayloadJson[A]] = deriveEncoder
  implicit def payloadJsonDecoder[A: Decoder]: Decoder[PayloadJson[A]] = deriveDecoder

}
