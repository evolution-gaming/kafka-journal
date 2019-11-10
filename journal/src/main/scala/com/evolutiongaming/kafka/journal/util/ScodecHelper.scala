package com.evolutiongaming.kafka.journal.util

import cats.MonadError
import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import play.api.libs.json.{Format, Json}
import scodec.bits.ByteVector
import scodec.{Attempt, Codec, Err, codecs}

import scala.util.Try

object ScodecHelper {

  implicit val attemptFailureFromStr: FromStr[Attempt.Failure] = (a: String) => Attempt.Failure(Err(a))

  implicit val attemptFailureToStr: ToStr[Attempt.Failure] = (a: Attempt.Failure) => a.cause.messageWithContext


  implicit val attemptMonadError: MonadError[Attempt, Attempt.Failure] = {

    import Attempt._

    new MonadError[Attempt, Failure] {

      def raiseError[A](a: Failure) = a

      def handleErrorWith[A](fa: Attempt[A])(f: Failure => Attempt[A]) = {
        fa match {
          case fa: Successful[A] => fa
          case fa: Failure       => f(fa)
        }
      }

      def pure[A](a: A) = Successful(a)

      def flatMap[A, B](fa: Attempt[A])(f: A => Attempt[B]) = fa.flatMap(f)

      def tailRecM[A, B](a: A)(f: A => Attempt[Either[A, B]]): Attempt[B] = {
        f(a) match {
          case b: Failure                  => b
          case b: Successful[Either[A, B]] => b.value match {
            case Left(b1) => tailRecM(b1)(f)
            case Right(a) => Successful(a)
          }
        }
      }
    }
  }


  implicit val attemptMonadString: MonadString[Attempt] = MonadString[Attempt, Attempt.Failure]


  def nelCodec[A](codec: Codec[List[A]]): Codec[Nel[A]] = {
    val to = (a: List[A]) => {
      Attempt.fromOption(a.toNel, Err("list is empty"))
    }
    val from = (a: Nel[A]) => Attempt.successful(a.toList)
    codec.exmap(to, from)
  }


  def formatCodec[A](implicit format: Format[A]): Codec[A] = {
    val fromBytes = (a: ByteVector) => {
      val jsValue = Try { Json.parse(a.toArray) }
      for {
        a <- Attempt.fromTry(jsValue)
        a <- format.reads(a).fold(a => Attempt.failure(Err(a.toString())), Attempt.successful)
      } yield a
    }
    val toBytes = (a: A) => {
      val jsValue = format.writes(a)
      val bytes = Json.toBytes(jsValue) // TODO unsafe
      val byteVector = ByteVector.view(bytes)
      Attempt.successful(byteVector)
    }
    codecs.bytes.exmap(fromBytes, toBytes)
  }
}
