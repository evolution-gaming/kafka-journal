package com.evolutiongaming.kafka.journal

import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.evolutiongaming.kafka.journal.Tags._
import com.evolutiongaming.kafka.journal.util.ScodecHelper._
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

    (Codec[SeqNr] :: Codec[Tags] :: payloadCodec).as[Event]
  }

  implicit val codecEvents: Codec[Nel[Event]] = {
    val eventsCodec = nelCodec(codecs.listOfN(codecs.int32, codecs.variableSizeBytes(codecs.int32, Codec[Event])))
    val version = ByteVector.fromByte(0)
    codecs.constant(version) ~> eventsCodec
  }


  implicit def eventsToBytes[F[_] : FromAttempt]: ToBytes[F, Nel[Event]] = ToBytes.fromEncoder

  implicit def eventsFromBytes[F[_] : FromAttempt]: FromBytes[F, Nel[Event]] = FromBytes.fromDecoder
}