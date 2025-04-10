package com.evolutiongaming.kafka.journal

import cats.*
import cats.syntax.all.*
import scodec.*
import scodec.bits.ByteVector

import scala.reflect.{ClassTag, TypeTest}
import scala.util.Try

final case class Event[A](seqNr: SeqNr, tags: Tags = Tags.empty, payload: Option[A] = None)

object Event {

  // TODO: WIP WTF???
  private implicit def typeTestOption[A <: Payload: ClassTag]: TypeTest[Option[Payload], Option[A]] =
    new TypeTest[Option[Payload], Option[A]] {
      override def unapply(x: Option[Payload]): Option[x.type & Option[A]] = x match {
        case Some(a: A) => a.some.asInstanceOf[x.type & Option[A]].some
        case _ => None
      }
    }

  implicit def codecEvent[A](
    implicit
    payloadCodec: Codec[Option[A]],
  ): Codec[Event[A]] =
    (SeqNr.codecSeqNr :: Tags.codecTags :: payloadCodec).as[Event[A]]

  implicit def codecEventPayload(
    implicit
    jsonCodec: JsonCodec[Try],
  ): Codec[Event[Payload]] = {

    val codecJson: Codec[Payload.Json] = Payload.Json.codecJson

    implicit val payloadCodec: Codec[Option[Payload]] = {

      val errEmpty = Err("")

      def codecSome[A](
        implicit
        codec: Codec[A],
      ) = {
        codec.exmap[Option[A]](a => Attempt.successful(a.some), a => Attempt.fromOption(a, errEmpty))
      }

      def codecOpt[A](payloadType: Byte, codec: Codec[Option[A]]) = {
        val byteVector = ByteVector.fromByte(payloadType)
        codecs.constant(byteVector) ~> codecs.variableSizeBytes(codecs.int32, codec)
      }

      val emptyCodec = codecOpt(0, codecs.provide(none[Payload]))

      val binaryCodec = codecOpt(1, codecSome[Payload.Binary])

      val jsonCodec = codecOpt(2, codecSome[Payload.Json](codecJson))

      val textCodec = codecOpt(3, codecSome[Payload.Text])

      codecs.choice[Option[Payload]](binaryCodec.upcast, jsonCodec.upcast, textCodec.upcast, emptyCodec)
    }

    codecEvent[Payload]
  }

  implicit val traverseEvent: Traverse[Event] = new Traverse[Event] {
    override def traverse[G[_]: Applicative, A, B](fa: Event[A])(f: A => G[B]): G[Event[B]] =
      fa.payload
        .traverse(f)
        .map(newPayload => fa.copy(payload = newPayload))

    override def foldLeft[A, B](fa: Event[A], b: B)(f: (B, A) => B): B =
      fa.payload.fold(b)(a => f(b, a))

    override def foldRight[A, B](fa: Event[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] =
      fa.payload.fold(lb)(a => f(a, lb))
  }
}
