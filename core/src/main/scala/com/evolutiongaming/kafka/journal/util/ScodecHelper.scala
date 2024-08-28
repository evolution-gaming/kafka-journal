package com.evolutiongaming.kafka.journal.util

import cats.MonadError
import cats.data.NonEmptyList as Nel
import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.JsonCodec
import play.api.libs.json.Format
import scodec.bits.ByteVector
import scodec.{Attempt, Codec, Err, codecs}

import java.nio.charset.{CharacterCodingException, StandardCharsets}
import scala.annotation.tailrec
import scala.util.Try

object ScodecHelper {

  implicit val attemptMonadError: MonadError[Attempt, Attempt.Failure] = {

    import Attempt.*

    new MonadError[Attempt, Failure] {

      def raiseError[A](a: Failure): Attempt[A] = a

      def handleErrorWith[A](fa: Attempt[A])(f: Failure => Attempt[A]): Attempt[A] = {
        fa match {
          case fa: Failure       => f(fa)
          case fa => fa
        }
      }

      def pure[A](a: A): Attempt[A] = Successful(a)

      def flatMap[A, B](fa: Attempt[A])(f: A => Attempt[B]): Attempt[B] = fa.flatMap(f)

      @tailrec //tailrec forces to use explicit match instead of fold and type erasure forces to use asInstanceOf
      def tailRecM[A, B](a: A)(f: A => Attempt[Either[A, B]]): Attempt[B] = {
        f(a) match {
          case failure: Failure => failure
          case success: Successful[?] =>
            success.value match {
              case Left(a1) => tailRecM(a1.asInstanceOf[A])(f)
              case Right(b) => Successful(b.asInstanceOf[B])
            }
        }
      }
    }
  }

  def nelCodec[A](codec: Codec[List[A]]): Codec[Nel[A]] = {
    val to = (a: List[A]) => {
      Attempt.fromOption(a.toNel, Err("list is empty"))
    }
    val from = (a: Nel[A]) => Attempt.successful(a.toList)
    codec.exmap(to, from)
  }

  def formatCodec[A](implicit format: Format[A], jsonCodec: JsonCodec[Try]): Codec[A] = {
    val fromBytes = (bytes: ByteVector) => {
      val jsValue = jsonCodec.decode.fromBytes(bytes)
      for {
        a <- Attempt.fromTry(jsValue)
        a <- format.reads(a).fold(a => Attempt.failure(Err(a.toString())), Attempt.successful)
      } yield a
    }
    val toBytes = (a: A) => {
      val jsValue = format.writes(a)
      val bytes   = jsonCodec.encode.toBytes(jsValue)
      Attempt.fromTry(bytes)
    }
    codecs.bytes.exmap(fromBytes, toBytes)
  }

  implicit class ByteVectorOps(val self: ByteVector) extends AnyVal {

    def decodeStr: Either[CharacterCodingException, String] = self.decodeString(StandardCharsets.UTF_8)
  }
}
