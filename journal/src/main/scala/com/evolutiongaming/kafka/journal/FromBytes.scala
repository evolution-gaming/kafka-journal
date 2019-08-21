package com.evolutiongaming.kafka.journal

import java.nio.charset.StandardCharsets.UTF_8

import play.api.libs.json.{JsValue, Json}

trait FromBytes[A] { self =>

  def apply(bytes: Bytes): A

  final def map[B](f: A => B): FromBytes[B] = (bytes: Bytes) => f(self(bytes))
}

object FromBytes {

  implicit val BytesFromBytes: FromBytes[Bytes] = (a: Bytes) => a

  implicit val StringFromBytes: FromBytes[String] = (a: Bytes) => new String(a, UTF_8)

  implicit val JsValueFromBytes: FromBytes[JsValue] = (a: Bytes) => Json.parse(a)

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