package com.evolutiongaming.kafka.journal

import cats.implicits._
import cats.{Applicative, Contravariant, ~>}
import play.api.libs.json.{JsValue, Json, Writes}
import scodec.bits.ByteVector
import scodec.{Encoder, codecs}


trait ToBytes[F[_], -A] {

  def apply(a: A): F[ByteVector]
}

object ToBytes {

  def apply[F[_], A](implicit F: ToBytes[F, A]): ToBytes[F, A] = F


  def const[F[_] : Applicative, A](bytes: ByteVector): ToBytes[F, A] = (_: A) => bytes.pure[F]


  def empty[F[_] : Applicative, A]: ToBytes[F, A] = const(ByteVector.empty)


  implicit def contravariantToBytes[F[_]]: Contravariant[ToBytes[F, ?]] = new Contravariant[ToBytes[F, ?]] {

    def contramap[A, B](fa: ToBytes[F, A])(f: B => A) = (a: B) => fa(f(a))
  }


  implicit def stringToBytes[F[_] : FromAttempt]: ToBytes[F, String] = fromEncoder(FromAttempt[F], codecs.utf8)

  implicit def byteVectorToBytes[F[_] : Applicative]: ToBytes[F, ByteVector] = _.pure[F]

  implicit def bytesToBytes[F[_] : Applicative]: ToBytes[F, Bytes] = (a: Bytes) => ByteVector.view(a).pure[F]

  implicit def jsValueToBytes[F[_] : Applicative]: ToBytes[F, JsValue] = fromWrites


  def fromEncoder[F[_] : FromAttempt, A](implicit encoder: Encoder[A]): ToBytes[F, A] = (a: A) => {
    val bytes = for {
      a <- encoder.encode(a)
    } yield {
      a.toByteVector
    }
    FromAttempt[F].apply { bytes }
  }


  def fromWrites[F[_] : Applicative, A](implicit writes: Writes[A]): ToBytes[F, A] = (a: A) => {
    val jsValue = writes.writes(a)
    val bytes = Json.toBytes(jsValue)
    val byteVector = ByteVector.view(bytes)
    byteVector.pure[F]
  }


  implicit class ToBytesOps[F[_], A](val self: ToBytes[F, A]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): ToBytes[G, A] = (a: A) => f(self(a))
  }


  object Implicits {

    implicit class ToBytesIdOps[A](val a: A) extends AnyVal {

      def toBytes[F[_]](implicit toBytes: ToBytes[F, A]): F[ByteVector] = toBytes(a)
    }
  }
}