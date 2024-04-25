package com.evolutiongaming.kafka.journal

import cats.data.{NonEmptyList => Nel}
import java.lang.{Byte => ByteJ}

import com.evolutiongaming.kafka.journal.util.ScodecHelper._
import scodec.bits.ByteVector
import scodec.{Codec, codecs}
import scodec.*

final case class Events[A](events: Nel[Event[A]], metadata: PayloadMetadata)

object Events {

  implicit def codecEvents[A](implicit
    eventCodec: Codec[Event[A]],
    metadataCodec: Codec[PayloadMetadata]
  ): Codec[Events[A]] = {
    val eventsCodec = nelCodec(codecs.listOfN(codecs.int32, codecs.variableSizeBytes(codecs.int32, eventCodec)))

    val default = (codecs.ignore(ByteJ.SIZE) ~> eventsCodec)
      .xmap[Events[A]](a => Events(a, PayloadMetadata.empty), _.events)

    val version0 = (codecs.constant(ByteVector.fromByte(0)) ~> eventsCodec)
      .xmap[Events[A]](a => Events(a, PayloadMetadata.empty), _.events)

    val version1 = (codecs.constant(ByteVector.fromByte(1)) ~> (eventsCodec :: metadataCodec))
      .as[Events[A]]

    codecs.choice(version1, version0, default)
  }

  implicit def eventsToBytes[F[_] : FromAttempt, A](implicit
    eventCodec: Codec[Event[A]],
    metadataCodec: Codec[PayloadMetadata]
  ): ToBytes[F, Events[A]] = ToBytes.fromEncoder

  implicit def eventsFromBytes[F[_] : FromAttempt, A](implicit
    eventCodec: Codec[Event[A]],
    metadataCodec: Codec[PayloadMetadata]
  ): FromBytes[F, Events[A]] = FromBytes.fromDecoder
}
