package com.evolution.kafka.journal

import cats.*
import cats.syntax.all.*
import scodec.*

import scala.util.Try

final case class Event[A](seqNr: SeqNr, tags: Tags = Tags.empty, payload: Option[A] = None)

object Event {

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

      // in the original codec, all subtypes were prefixed with an int32 length, even the None option
      def sizePrefixed[A](codec: Codec[A]): Codec[A] = codecs.variableSizeBytes(codecs.int32, codec)

      codecs.discriminated[Option[Payload]].by(codecs.uint8)
        .caseP(0) { case None => None }(identity)(sizePrefixed(codecs.provide(None)))
        .caseP(1) { case Some(binary: Payload.Binary) => binary }(_.some)(sizePrefixed(Codec[Payload.Binary]))
        .caseP(2) { case Some(json: Payload.Json) => json }(_.some)(sizePrefixed(codecJson))
        .caseP(3) { case Some(text: Payload.Text) => text }(_.some)(sizePrefixed(Codec[Payload.Text]))
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
