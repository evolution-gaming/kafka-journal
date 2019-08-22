package com.evolutiongaming.kafka.journal

import java.nio.charset.StandardCharsets.UTF_8

import play.api.libs.json.{JsValue, Json}
import scodec.Decoder
import scodec.bits.{BitVector, ByteVector}

trait FromBytes[A] { self =>

  def apply(bytes: Bytes): A

  final def map[B](f: A => B): FromBytes[B] = (bytes: Bytes) => f(self(bytes))
}

object FromBytes {

  implicit val BytesFromBytes: FromBytes[Bytes] = (a: Bytes) => a

  implicit val StringFromBytes: FromBytes[String] = (a: Bytes) => new String(a, UTF_8)

  implicit val JsValueFromBytes: FromBytes[JsValue] = (a: Bytes) => Json.parse(a)

  implicit val BytesVectorToBytes: FromBytes[ByteVector] = (a: Bytes) => ByteVector.view(a)


  def apply[A](implicit fromBytes: FromBytes[A]): FromBytes[A] = fromBytes

  def const[A](a: A): FromBytes[A] = (_: Bytes) => a


  implicit def decoderFromBytes[A](implicit decoder: Decoder[A]): FromBytes[A] = {
    a: Bytes => {
      val bitVector = BitVector(a)
      decoder.decode(bitVector).require.value
    }
  }


  object Implicits {

    implicit class FromBytesIdOps(val bytes: Bytes) extends AnyVal {
      def fromBytes[A](implicit fromBytes: FromBytes[A]): A = fromBytes(bytes)
    }
  }
}