package com.evolutiongaming.kafka.journal

import java.nio.charset.StandardCharsets.UTF_8

import play.api.libs.json.{JsValue, Json}

trait FromBytes[A] { self =>

  def apply(bytes: Bytes): A

  final def map[B](f: A => B): FromBytes[B] = new FromBytes[B] {
    def apply(bytes: Bytes) = f(self(bytes))
  }
}

object FromBytes {

  implicit val BytesImpl: FromBytes[Bytes] = new FromBytes[Bytes] {
    def apply(bytes: Bytes) = bytes
  }

  implicit val StrImpl: FromBytes[String] = new FromBytes[String] {
    def apply(bytes: Bytes) = new String(bytes, UTF_8)
  }

  implicit val JsonImpl: FromBytes[JsValue] = new FromBytes[JsValue] {
    def apply(bytes: Bytes) = Json.parse(bytes)
  }

  def apply[A](implicit fromBytes: FromBytes[A]): FromBytes[A] = fromBytes

  def const[A](a: A): FromBytes[A] = new FromBytes[A] {
    def apply(bytes: Bytes) = a
  }


  object Implicits {

    implicit class FromBytesIdOps(val bytes: Bytes) extends AnyVal {
      def fromBytes[A](implicit fromBytes: FromBytes[A]): A = fromBytes(bytes)
    }
  }
}