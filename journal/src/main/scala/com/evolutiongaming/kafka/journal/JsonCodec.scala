package com.evolutiongaming.kafka.journal

import cats.Applicative
import cats.implicits._
import com.evolutiongaming.catshelper.FromTry
import com.evolutiongaming.jsonitertool.PlayJsonJsoniter
import play.api.libs.json.{JsValue, Json}
import scodec.bits.ByteVector

import scala.util.Try

final case class JsonCodec[F[_]](encode: JsonCodec.Encode[F], decode: JsonCodec.Decode[F])

object JsonCodec {

  def apply[F[_]](implicit ev: JsonCodec[F]): JsonCodec[F] = ev

  final case class Encode[F[_]](toBytes: ToBytes[F, JsValue])

  object Encode {
    implicit def fromCodec[F[_]](implicit codec: JsonCodec[F]): Encode[F] = codec.encode
  }

  final case class Decode[F[_]](fromBytes: FromBytes[F, JsValue])

  object Decode {
    implicit def fromCodec[F[_]](implicit codec: JsonCodec[F]): Decode[F] = codec.decode
  }

  def playJson[F[_] : Applicative : FromTry]: JsonCodec[F] = JsonCodec(

    encode = Encode { value =>
      ByteVector(Json.toBytes(value)).pure[F]
    },

    decode = Decode { bytes =>
      lift {
        Try(Json.parse(bytes.toArray))
      }
    }
  )

  def jsoniter[F[_] : Applicative : FromTry]: JsonCodec[F] = JsonCodec(

    encode = Encode { value =>
      ByteVector(PlayJsonJsoniter.serialize(value)).pure[F]
    },

    decode = Decode { bytes =>
      lift {
        PlayJsonJsoniter.deserialize(bytes.toArray)
      }
    }
  )

  @inline
  private def lift[F[_]: FromTry](result: Try[JsValue]): F[JsValue] =
    FromTry[F].apply {
      result.adaptErr {
        case e => JournalError(s"Failed to parse json: ${e.getMessage}", e)
      }
    }
}
