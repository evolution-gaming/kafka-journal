package com.evolutiongaming.kafka.journal

import cats.implicits._
import cats.Applicative
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper.{ApplicativeThrowable, FromTry}
import com.evolutiongaming.jsonitertool.PlayJsonJsoniter
import play.api.libs.json.{JsValue, Json}
import scodec.bits.ByteVector

import scala.util.Try

final case class JsonCodec[F[_]](
  encode: JsonCodec.Encode[F],
  decode: JsonCodec.Decode[F])

object JsonCodec {

  def apply[F[_]](implicit F: JsonCodec[F]): JsonCodec[F] = F


  def playJson[F[_] : Applicative : FromTry]: JsonCodec[F] = {
    JsonCodec(
      encode = Encode.playJson,
      decode = Decode.playJson)
  }


  def jsoniter[F[_] : Applicative : FromTry]: JsonCodec[F] = {
    JsonCodec(
      encode = Encode.jsoniter,
      decode = Decode.jsoniter)
  }


  def default[F[_] : ApplicativeThrowable : FromTry]: JsonCodec[F] = {
    JsonCodec(
      encode = Encode.jsoniter,
      decode = Decode.jsoniter fallbackTo Decode.playJson)
  }


  final case class Encode[F[_]](toBytes: ToBytes[F, JsValue])

  object Encode {

    implicit def fromCodec[F[_]](implicit codec: JsonCodec[F]): Encode[F] = codec.encode


    def playJson[F[_] : Applicative : FromTry]: Encode[F] = {
      Encode { value =>
        ByteVector(Json.toBytes(value)).pure[F]
      }
    }


    def jsoniter[F[_] : Applicative : FromTry]: Encode[F] = {
      Encode { value =>
        ByteVector(PlayJsonJsoniter.serialize(value)).pure[F]
      }
    }
  }


  final case class Decode[F[_]](fromBytes: FromBytes[F, JsValue])

  object Decode {

    implicit def fromCodec[F[_]](implicit codec: JsonCodec[F]): Decode[F] = codec.decode


    def playJson[F[_] : FromTry]: Decode[F] = {
      Decode { bytes =>
        lift(bytes) {
          Try { Json.parse(bytes.toArray) }
        }
      }
    }


    def jsoniter[F[_] : FromTry]: Decode[F] = {
      Decode { bytes =>
        lift(bytes) {
          PlayJsonJsoniter.deserialize(bytes.toArray)
        }
      }
    }


    implicit class DecodeOps[F[_]](val self: Decode[F]) extends AnyVal {

      def fallbackTo(decode: Decode[F])(implicit F: ApplicativeThrowable[F]): Decode[F] = {
        Decode { bytes =>
          self
            .fromBytes(bytes)
            .handleErrorWith { e =>
              decode
                .fromBytes(bytes)
                .adaptErr { case _ => e }
            }
        }
      }
    }
  }


  private def lift[F[_] : FromTry](bytes: ByteVector)(result: Try[JsValue]): F[JsValue] = {
    result
      .adaptErr { case e => JournalError(s"Failed to parse $bytes json: $e", e) }
      .fromTry[F]
  }
}
