package com.evolutiongaming.kafka.journal.util

import cats.MonadError
import cats.data.NonEmptyList as Nel
import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.JsonCodec
import play.api.libs.json.Format
import scodec.*
import scodec.bits.ByteVector

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
          case fa: Successful[A @unchecked] => fa
          case fa: Failure => f(fa)
        }
      }

      def pure[A](a: A): Attempt[A] = Successful(a)

      def flatMap[A, B](fa: Attempt[A])(f: A => Attempt[B]): Attempt[B] = fa.flatMap(f)

      @tailrec
      def tailRecM[A, B](a: A)(f: A => Attempt[Either[A, B]]): Attempt[B] = {
        f(a) match {
          case b: Failure => b
          case b: Successful[Either[A, B] @unchecked] =>
            b.value match {
              case Left(b1) => tailRecM(b1)(f)
              case Right(a) => Successful(a)
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

  def formatCodec[A](
    implicit
    format: Format[A],
    jsonCodec: JsonCodec[Try],
  ): Codec[A] = {
    val fromBytes = (bytes: ByteVector) => {
      val jsValue = jsonCodec.decode.fromBytes(bytes)
      for {
        a <- Attempt.fromTry(jsValue)
        a <- format.reads(a).fold(a => Attempt.failure(Err(a.toString())), Attempt.successful)
      } yield a
    }
    val toBytes = (a: A) => {
      val jsValue = format.writes(a)
      val bytes = jsonCodec.encode.toBytes(jsValue)
      Attempt.fromTry(bytes)
    }
    codecs.bytes.exmap(fromBytes, toBytes)
  }

  implicit class ByteVectorOps(val self: ByteVector) extends AnyVal {

    def decodeStr: Either[CharacterCodingException, String] = self.decodeString(StandardCharsets.UTF_8)
  }
}
