package com.evolution.kafka.journal

import cats.syntax.all.*
import cats.~>
import com.evolution.kafka.journal.util.ScodecHelper.*
import com.evolution.playjson.jsoniter.PlayJsonJsoniter
import com.evolutiongaming.catshelper.CatsHelper.*
import com.evolutiongaming.catshelper.{ApplicativeThrowable, FromTry, MonadThrowable}
import play.api.libs.json.{JsValue, Json}
import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets
import scala.util.Try

final case class JsonCodec[F[_]](encode: JsonCodec.Encode[F], decode: JsonCodec.Decode[F])

object JsonCodec {

  def summon[F[_]](
    implicit
    F: JsonCodec[F],
  ): JsonCodec[F] = F

  def playJson[F[_]: FromTry]: JsonCodec[F] = {
    JsonCodec(encode = Encode.playJson, decode = Decode.playJson)
  }

  def jsoniter[F[_]: FromTry]: JsonCodec[F] = {
    JsonCodec(encode = Encode.jsoniter, decode = Decode.jsoniter)
  }

  def default[F[_]: ApplicativeThrowable: FromTry]: JsonCodec[F] = {
    JsonCodec(
      encode = Encode.jsoniter[F] fallbackTo Encode.playJson,
      decode = Decode.jsoniter[F] fallbackTo Decode.playJson,
    )
  }

  /**
   * The same as [[default]], except that the types having a streaming decoder are decoded straight
   * from bytes, without building a `JsValue` tree first. Currently, it is only `ActionHeader`,
   * parsed from every record consumed by the head cache and when reading a journal.
   *
   * A streaming decoder only handles the JSON written by the journal, and falls back to decoding
   * the tree with [[default]] otherwise, so the results are the same as with [[default]].
   */
  def streaming[F[_]: ApplicativeThrowable: FromTry]: JsonCodec[F] = {
    JsonCodec(encode = default[F].encode, decode = Decode.streaming)
  }

  implicit class JsonCodecOps[F[_]](val self: JsonCodec[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): JsonCodec[G] = JsonCodec(self.encode.mapK(f), self.decode.mapK(f))
  }

  final case class Encode[F[_]](toBytes: ToBytes[F, JsValue])

  object Encode {

    def summon[F[_]](
      implicit
      F: Encode[F],
    ): Encode[F] = F

    implicit def fromCodec[F[_]](
      implicit
      codec: JsonCodec[F],
    ): Encode[F] = codec.encode

    def playJson[F[_]: FromTry]: Encode[F] = {
      Encode(new ToBytes[F, JsValue] {
        def apply(value: JsValue): F[ByteVector] = {
          FromTry[F].unsafe {
            val bytes = Json.toBytes(value)
            ByteVector.view(bytes)
          }
        }
      })
    }

    def jsoniter[F[_]: FromTry]: Encode[F] = {
      Encode(new ToBytes[F, JsValue] {
        def apply(value: JsValue): F[ByteVector] = {
          FromTry[F].unsafe {
            val bytes = PlayJsonJsoniter.serialize(value)
            ByteVector.view(bytes)
          }
        }
      })
    }

    implicit class EncodeOps[F[_]](val self: Encode[F]) extends AnyVal {

      def mapK[G[_]](f: F ~> G): Encode[G] = Encode(self.toBytes.mapK(f))

      def fallbackTo(
        encode: Encode[F],
      )(implicit
        F: ApplicativeThrowable[F],
      ): Encode[F] = {
        Encode(new ToBytes[F, JsValue] {
          def apply(jsValue: JsValue): F[ByteVector] = {
            self
              .toBytes(jsValue)
              .handleErrorWith { e =>
                encode
                  .toBytes(jsValue)
                  .adaptErr { case _ => e }
              }
          }
        })
      }

      def toStr(
        value: JsValue,
      )(implicit
        F: MonadThrowable[F],
      ): F[String] =
        for {
          bytes <- self.toBytes(value)
          str <- bytes.decodeStr.liftTo[F]
        } yield str
    }
  }

  final case class Decode[F[_]](fromBytes: FromBytes[F, JsValue])

  object Decode {

    def summon[F[_]](
      implicit
      F: Decode[F],
    ): Decode[F] = F

    implicit def fromCodec[F[_]](
      implicit
      codec: JsonCodec[F],
    ): Decode[F] = codec.decode

    def playJson[F[_]: FromTry]: Decode[F] = {
      Decode(new FromBytes[F, JsValue] {
        def apply(byteVector: ByteVector): F[JsValue] = {
          val bytes = byteVector.toArray
          Try { Json.parse(bytes) }.adapt(byteVector)
        }
      })
    }

    def jsoniter[F[_]: FromTry]: Decode[F] = {
      Decode(new FromBytes[F, JsValue] {
        def apply(byteVector: ByteVector): F[JsValue] = {
          PlayJsonJsoniter
            .deserialize(byteVector.toArray)
            .adapt(byteVector)
        }
      })
    }

    /**
     * @see
     *   [[JsonCodec#streaming]]
     */
    def streaming[F[_]: ApplicativeThrowable: FromTry]: Decode[F] = {
      Decode(new Streaming(default[F].decode.fromBytes))
    }

    /**
     * Decodes `JsValue` with `tree`, and tells the types having a streaming decoder to use it, see
     * [[JsonCodec#streaming]].
     *
     * It is a marker rather than a field of [[Decode]], so that [[Decode]] stays binary compatible.
     */
    private[journal] final class Streaming[F[_]](val tree: FromBytes[F, JsValue]) extends FromBytes[F, JsValue] {

      def apply(bytes: ByteVector): F[JsValue] = tree(bytes)
    }

    implicit class DecodeOps[F[_]](val self: Decode[F]) extends AnyVal {

      def mapK[G[_]](f: F ~> G): Decode[G] = {
        self.fromBytes match {
          case streaming: Streaming[F @unchecked] => Decode(new Streaming(streaming.tree.mapK(f)))
          case fromBytes => Decode(fromBytes.mapK(f))
        }
      }

      /**
       * Whether the types having a streaming decoder should use it, see [[JsonCodec#streaming]].
       */
      def isStreaming: Boolean = self.fromBytes.isInstanceOf[Streaming[F @unchecked]]

      def fallbackTo(
        decode: Decode[F],
      )(implicit
        F: ApplicativeThrowable[F],
      ): Decode[F] = {
        Decode(new FromBytes[F, JsValue] {
          def apply(bytes: ByteVector): F[JsValue] = {
            self
              .fromBytes(bytes)
              .handleErrorWith { e =>
                decode
                  .fromBytes(bytes)
                  .adaptErr { case _ => e }
              }
          }
        })
      }

      def fromStr(
        str: String,
      )(implicit
        F: MonadThrowable[F],
      ): F[JsValue] =
        for {
          bytes <- ByteVector.encodeString(str)(StandardCharsets.UTF_8).liftTo[F]
          value <- self.fromBytes(bytes)
        } yield value
    }
  }

  private implicit class TryOps[A](val self: Try[A]) extends AnyVal {

    def adapt[F[_]: FromTry](byteVector: ByteVector): F[A] = {
      self
        .adaptErr {
          case e =>
            val str = byteVector
              .decodeStr
              .getOrElse { byteVector.toString() }
            JournalError(s"failed to parse $str: $e", e)
        }
        .fromTry[F]
    }
  }
}
