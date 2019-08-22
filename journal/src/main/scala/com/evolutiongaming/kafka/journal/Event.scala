package com.evolutiongaming.kafka.journal

import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.evolutiongaming.kafka.journal.Tags._
import com.evolutiongaming.kafka.journal.util.ScodecHelper._
import scodec.bits.BitVector
import scodec.{Attempt, Codec, Err, codecs}

final case class Event(
  seqNr: SeqNr,
  tags: Tags = Tags.Empty,
  payload: Option[Payload] = None)

object Event {

  implicit val CodecEvent: Codec[Event] = {
    val payloadCodec = {

      val errEmpty = Err("")

      def codecSome[A](implicit codec: Codec[A]) = {
        codec.exmap[Option[A]](
          a => Attempt.successful(a.some),
          a => Attempt.fromOption(a, errEmpty))
      }

      def codecOpt[A](payloadType: Byte, codec: Codec[Option[A]]) = {
        val bitVector = BitVector.fromByte(payloadType)
        codecs.constant(bitVector) ~> codecs.variableSizeBytes(codecs.int32, codec)
      }

      val emptyCodec = codecOpt(0, codecs.provide(none[Payload]))

      val binaryCodec = codecOpt(1, codecSome[Payload.Binary])

      val jsonCodec = codecOpt(2, codecSome[Payload.Json])

      val textCodec = codecOpt(3, codecSome[Payload.Text])

      codecs.choice[Option[Payload]](
        binaryCodec.upcast,
        jsonCodec.upcast,
        textCodec.upcast,
        emptyCodec)
    }

    (Codec[SeqNr] :: Codec[Tags] :: payloadCodec).as[Event]
  }

  implicit val CodecEvents: Codec[Nel[Event]] = {
    val eventsCodec = nelCodec(codecs.listOfN(codecs.int32, codecs.variableSizeBytes(codecs.int32, Codec[Event])))
    val version = BitVector.fromByte(0)
    codecs.constant(version) ~> eventsCodec
  }


  implicit val EventsToBytes: ToBytes[Nel[Event]] = ToBytes.encoderToBytes[Nel[Event]]

  implicit val EventsFromBytes: FromBytes[Nel[Event]] = FromBytes.decoderFromBytes[Nel[Event]]
}