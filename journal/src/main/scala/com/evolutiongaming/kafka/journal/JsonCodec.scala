package com.evolutiongaming.kafka.journal

import java.nio.charset.StandardCharsets

import cats.implicits._
import cats.{Applicative, ~>}
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper.{ApplicativeThrowable, FromTry, MonadThrowable}
import com.evolutiongaming.jsonitertool.PlayJsonJsoniter
import play.api.libs.json.{JsValue, Json}
import scodec.bits.ByteVector

import scala.util.Try

final case class JsonCodec[F[_]](
  encode: JsonCodec.Encode[F],
  decode: JsonCodec.Decode[F])

object JsonCodec {

  def summon[F[_]](implicit F: JsonCodec[F]): JsonCodec[F] = F


  def playJson[F[_]: Applicative: FromTry]: JsonCodec[F] = {
    JsonCodec(
      encode = Encode.playJson,
      decode = Decode.playJson)
  }


  def jsoniter[F[_]: Applicative: FromTry]: JsonCodec[F] = {
    JsonCodec(
      encode = Encode.jsoniter,
      decode = Decode.jsoniter)
  }


  def default[F[_]: ApplicativeThrowable: FromTry]: JsonCodec[F] = {
    JsonCodec(
      encode = Encode.jsoniter[F] fallbackTo Encode.playJson,
      decode = Decode.jsoniter[F] fallbackTo Decode.playJson)
  }


  implicit class JsonCodecOps[F[_]](val self: JsonCodec[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): JsonCodec[G] = JsonCodec(self.encode.mapK(f), self.decode.mapK(f))
  }


  final case class Encode[F[_]](toBytes: ToBytes[F, JsValue])

  object Encode {

    def summon[F[_]](implicit F: Encode[F]): Encode[F] = F

    implicit def fromCodec[F[_]](implicit codec: JsonCodec[F]): Encode[F] = codec.encode


    def playJson[F[_]: Applicative: FromTry]: Encode[F] = {
      Encode { value =>
        FromTry[F].unsafe {
          ByteVector.view(Json.toBytes(value))
        }
      }
    }


    def jsoniter[F[_]: Applicative: FromTry]: Encode[F] = {
      Encode { value =>
        FromTry[F].unsafe {
          ByteVector.view(PlayJsonJsoniter.serialize(value))
        }
      }
    }


    implicit class EncodeOps[F[_]](val self: Encode[F]) extends AnyVal {

      def mapK[G[_]](f: F ~> G): Encode[G] = Encode(self.toBytes.mapK(f))

      def fallbackTo(encode: Encode[F])(implicit F: ApplicativeThrowable[F]): Encode[F] = {
        Encode { jsValue =>
          self
            .toBytes(jsValue)
            .handleErrorWith { e =>
              encode
                .toBytes(jsValue)
                .adaptErr { case _ => e }
            }
        }
      }

      def toStr(value: JsValue)(implicit F: MonadThrowable[F]): F[String] =
        for {
          bytes <- self.toBytes(value)
          str   <- bytes.decodeString(StandardCharsets.UTF_8).liftTo[F]
        } yield str
    }
  }


  final case class Decode[F[_]](fromBytes: FromBytes[F, JsValue])

  object Decode {

    def summon[F[_]](implicit F: Decode[F]): Decode[F] = F

    implicit def fromCodec[F[_]](implicit codec: JsonCodec[F]): Decode[F] = codec.decode


    def playJson[F[_]: FromTry]: Decode[F] = {
      Decode { bytes =>
        lift(bytes) {
          Try { Json.parse(bytes.toArray) }
        }
      }
    }


    def jsoniter[F[_]: FromTry]: Decode[F] = {
      Decode { bytes =>
        lift(bytes) {
          PlayJsonJsoniter.deserialize(bytes.toArray)
        }
      }
    }


    implicit class DecodeOps[F[_]](val self: Decode[F]) extends AnyVal {

      def mapK[G[_]](f: F ~> G): Decode[G] = Decode(self.fromBytes.mapK(f))

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

      def fromStr(str: String)(implicit F: MonadThrowable[F]): F[JsValue] =
        for {
          bytes <- ByteVector.encodeString(str)(StandardCharsets.UTF_8).liftTo[F]
          value <- self.fromBytes(bytes)
        } yield value
    }
  }


  private def lift[F[_]: FromTry](bytes: ByteVector)(result: Try[JsValue]): F[JsValue] = {
    result
      .adaptErr { case e => JournalError(s"Failed to parse $bytes json: $e", e) }
      .fromTry[F]
  }
}
