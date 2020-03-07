package com.evolutiongaming.kafka.journal

import cats.implicits._
import scodec.bits.ByteVector
import scodec.{Attempt, Codec, Err, codecs}

final case class Event(
  seqNr: SeqNr,
  tags: Tags = Tags.empty,
  payload: Option[Payload] = None)

object Event {

  implicit val codecEvent: Codec[Event] = {
    val payloadCodec = {

      val errEmpty = Err("")

      def codecSome[A](implicit codec: Codec[A]) = {
        codec.exmap[Option[A]](
          a => Attempt.successful(a.some),
          a => Attempt.fromOption(a, errEmpty))
      }

      def codecOpt[A](payloadType: Byte, codec: Codec[Option[A]]) = {
        val byteVector = ByteVector.fromByte(payloadType)
        codecs.constant(byteVector) ~> codecs.variableSizeBytes(codecs.int32, codec)
      }

      val emptyCodec = codecOpt(0, codecs.provide(none[Payload]))

      val binaryCodec = codecOpt(1, codecSome[Payload.Binary])

      val jsonCodec = codecOpt(2, codecSome[Payload.Json](Payload.Json.codecJson))

      val textCodec = codecOpt(3, codecSome[Payload.Text])

      codecs.choice[Option[Payload]](
        binaryCodec.upcast,
        jsonCodec.upcast,
        textCodec.upcast,
        emptyCodec)
    }

    (SeqNr.codecSeqNr :: Tags.codecTags :: payloadCodec).as[Event]
  }
}