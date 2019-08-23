package com.evolutiongaming.kafka.journal

import java.nio.charset.StandardCharsets.UTF_8

import play.api.libs.json.{JsValue, Json, Writes}
import scodec.Encoder
import scodec.bits.ByteVector

// TODO add F
trait ToBytes[-A] { self =>

  def apply(a: A): Bytes

  final def imap[B](f: B => A): ToBytes[B] = (b: B) => self(f(b))
}

object ToBytes {

  private val Empty = const(Array.empty)

  implicit val BytesToBytes: ToBytes[Bytes] = (a: Bytes) => a

  implicit val StringToBytes: ToBytes[String] = (a: String) => a.getBytes(UTF_8)

  implicit val JsValueToBytes: ToBytes[JsValue] = fromWrites

  implicit val BytesVectorToBytes: ToBytes[ByteVector] = _.toArray


  def apply[A](implicit toBytes: ToBytes[A]): ToBytes[A] = toBytes

  def empty[A]: ToBytes[A] = Empty

  def const[A](bytes: Bytes): ToBytes[A] = (_: A) => bytes


  def fromEncoder[A](implicit encoder: Encoder[A]): ToBytes[A] = (a: A) => {
    val attempt = encoder.encode(a)
    attempt.require.toByteArray
  }


  def fromWrites[A](implicit writes: Writes[A]): ToBytes[A] = (a: A) => {
    val jsValue = writes.writes(a)
    Json.toBytes(jsValue)
  }


  object Implicits {

    implicit class ToBytesIdOps[A](val a: A) extends AnyVal {
      def toBytes(implicit toBytes: ToBytes[A]): Bytes = toBytes(a)
    }
  }
}