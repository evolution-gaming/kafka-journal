package com.evolutiongaming.kafka.journal

import java.nio.charset.StandardCharsets.UTF_8

import play.api.libs.json.{JsValue, Json}

// TODO add F
trait ToBytes[-A] { self =>

  def apply(a: A): Bytes

  final def imap[B](f: B => A): ToBytes[B] = new ToBytes[B] {
    def apply(b: B) = self(f(b))
  }
}

object ToBytes {

  private val Empty = const(Array.empty)

  implicit val BytesToBytes: ToBytes[Bytes] = new ToBytes[Bytes] {
    def apply(a: Bytes) = a
  }

  implicit val StringToBytes: ToBytes[String] = new ToBytes[String] {
    def apply(a: String) = a.getBytes(UTF_8)
  }

  implicit val JsValueToBytes: ToBytes[JsValue] = new ToBytes[JsValue] {
    def apply(a: JsValue) = Json.toBytes(a)
  }

  def apply[A](implicit toBytes: ToBytes[A]): ToBytes[A] = toBytes

  def empty[A]: ToBytes[A] = Empty

  def const[A](bytes: Bytes): ToBytes[A] = new ToBytes[A] {
    def apply(a: A) = bytes
  }


  object Implicits {

    implicit class ToBytesIdOps[A](val a: A) extends AnyVal {
      def toBytes(implicit toBytes: ToBytes[A]): Bytes = toBytes(a)
    }
  }
}