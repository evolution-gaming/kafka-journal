package com.evolution.kafka.journal

import cats.syntax.all.*
import cats.{Applicative, Functor, Monad, ~>}
import play.api.libs.json.*
import scodec.*
import scodec.bits.ByteVector

trait FromBytes[F[_], A] {

  def apply(bytes: ByteVector): F[A]
}

object FromBytes {

  def apply[F[_], A](
    implicit
    F: FromBytes[F, A],
  ): FromBytes[F, A] = F

  def const[F[_]: Applicative, A](a: A): FromBytes[F, A] = new FromBytes[F, A] {
    def apply(bytes: ByteVector): F[A] = a.pure[F]
  }

  implicit def functorFromBytes[F[_]: Functor]: Functor[FromBytes[F, *]] = new Functor[FromBytes[F, *]] {

    def map[A, B](fa: FromBytes[F, A])(f: A => B): FromBytes[F, B] = new FromBytes[F, B] {
      def apply(a: ByteVector): F[B] = fa(a).map(f)
    }
  }

  implicit def byteVectorFromBytes[F[_]: Applicative]: FromBytes[F, ByteVector] = new FromBytes[F, ByteVector] {
    def apply(a: ByteVector): F[ByteVector] = a.pure[F]
  }

  implicit def stringFromBytes[F[_]: FromAttempt]: FromBytes[F, String] = new FromBytes[F, String] {
    def apply(a: ByteVector): F[String] = {
      val as = for {
        a <- codecs.utf8.decode(a.toBitVector)
      } yield {
        a.value
      }
      FromAttempt[F].apply(as)
    }
  }

  def fromDecoder[F[_]: FromAttempt, A](
    implicit
    decoder: Decoder[A],
  ): FromBytes[F, A] = new FromBytes[F, A] {
    def apply(a: ByteVector): F[A] = {
      FromAttempt[F].apply {
        for {
          a <- decoder.decode(a.toBitVector)
        } yield {
          a.value
        }
      }
    }
  }

  def fromReads[F[_]: Monad: FromJsResult, A](
    implicit
    reads: Reads[A],
    decode: JsonCodec.Decode[F],
  ): FromBytes[F, A] = new FromBytes[F, A] {
    def apply(bytes: ByteVector): F[A] = {
      for {
        a <- decode.fromBytes(bytes)
        a <- FromJsResult[F].apply(reads.reads(a))
      } yield a
    }
  }

  implicit class FromBytesOps[F[_], A](val self: FromBytes[F, A]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): FromBytes[G, A] = new FromBytes[G, A] {
      def apply(a: ByteVector): G[A] = f(self(a))
    }
  }

  object implicits {

    implicit class ByteVectorFromBytesOps(val self: ByteVector) extends AnyVal {

      def fromBytes[F[_], A](
        implicit
        fromBytes: FromBytes[F, A],
      ): F[A] = fromBytes(self)
    }
  }
}
