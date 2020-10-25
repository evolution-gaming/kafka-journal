package com.evolutiongaming.kafka.journal

import cats.syntax.all._
import cats.{Applicative, Functor, Monad, ~>}
import play.api.libs.json._
import scodec.bits.ByteVector
import scodec.{Decoder, codecs}

trait FromBytes[F[_], A] {

  def apply(bytes: ByteVector): F[A]
}

object FromBytes {

  def apply[F[_], A](implicit F: FromBytes[F, A]): FromBytes[F, A] = F


  def const[F[_] : Applicative, A](a: A): FromBytes[F, A] = (_: ByteVector) => a.pure[F]


  implicit def functorFromBytes[F[_] : Functor]: Functor[FromBytes[F, ?]] = new Functor[FromBytes[F, ?]] {

    def map[A, B](fa: FromBytes[F, A])(f: A => B) = (a: ByteVector) => fa(a).map(f)
  }

  implicit def byteVectorFromBytes[F[_] : Applicative]: FromBytes[F, ByteVector] = (a: ByteVector) => a.pure[F]

  implicit def stringFromBytes[F[_] : FromAttempt]: FromBytes[F, String] = (a: ByteVector) => {
    val as = for {
      a <- codecs.utf8.decode(a.toBitVector)
    } yield {
      a.value
    }
    FromAttempt[F].apply(as)
  }


  def fromDecoder[F[_] : FromAttempt, A](implicit decoder: Decoder[A]): FromBytes[F, A] = (a: ByteVector) => {
    FromAttempt[F].apply {
      for {
        a <- decoder.decode(a.toBitVector)
      } yield {
        a.value
      }
    }
  }

  def fromReads[F[_]: Monad: FromJsResult, A](
    implicit reads: Reads[A],
    decode: JsonCodec.Decode[F]
  ): FromBytes[F, A] = {
    bytes =>
      for {
        a <- decode.fromBytes(bytes)
        a <- FromJsResult[F].apply(reads.reads(a))
      } yield a
  }


  implicit class FromBytesOps[F[_], A](val self: FromBytes[F, A]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): FromBytes[G, A] = (a: ByteVector) => f(self(a))
  }


  object implicits {

    implicit class ByteVectorFromBytesOps(val self: ByteVector) extends AnyVal {

      def fromBytes[F[_], A](implicit fromBytes: FromBytes[F, A]): F[A] = fromBytes(self)
    }
  }
}