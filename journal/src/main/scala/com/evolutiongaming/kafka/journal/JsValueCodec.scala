package com.evolutiongaming.kafka.journal

import cats.Applicative
import cats.implicits._
import com.evolutiongaming.catshelper.FromTry
import com.evolutiongaming.jsonitertool.PlayJsonJsoniter
import play.api.libs.json.{JsValue, Json}
import scodec.bits.ByteVector

import scala.util.Try

case class JsValueCodec[F[_]](encode: JsValueCodec.Encode[F], decode: JsValueCodec.Decode[F])

object JsValueCodec {

  def apply[F[_]](implicit ev: JsValueCodec[F]): JsValueCodec[F] = ev

  case class Encode[F[_]](toBytes: ToBytes[F, JsValue])

  object Encode {
    implicit def fromCodec[F[_]](implicit codec: JsValueCodec[F]): Encode[F] = codec.encode
  }

  case class Decode[F[_]](fromBytes: FromBytes[F, JsValue])

  object Decode {
    implicit def fromCodec[F[_]](implicit codec: JsValueCodec[F]): Decode[F] = codec.decode
  }

  def playJson[F[_] : Applicative : FromTry]: JsValueCodec[F] = JsValueCodec(

    encode = Encode { value =>
      ByteVector(Json.toBytes(value)).pure[F]
    },

    decode = Decode { bytes =>
      lift {
        Try(Json.parse(bytes.toArray))
      }
    }
  )

  def jsoniter[F[_] : Applicative : FromTry]: JsValueCodec[F] = JsValueCodec(

    encode = Encode { value =>
      ByteVector(PlayJsonJsoniter.serialize(value)).pure[F]
    },

    decode = Decode { bytes =>
      lift {
        PlayJsonJsoniter.deserialize(bytes.toArray)
      }
    }
  )

  object Implicits {

    implicit def default[F[_] : Applicative : FromTry]: JsValueCodec[F] = JsValueCodec.playJson
  }

  class JsonParsingError(cause: Throwable) extends Throwable(s"failed to parse json ${cause.getMessage}", cause)

  @inline
  private def lift[F[_]: FromTry](result: Try[JsValue]): F[JsValue] =
    FromTry[F].apply {
      result.adaptErr {
        case e => new JsonParsingError(e)
      }
    }
}
