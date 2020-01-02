package com.evolutiongaming.kafka.journal

import cats.data.{NonEmptyList => Nel}
import java.lang.{Byte => ByteJ}

import com.evolutiongaming.kafka.journal.util.ScodecHelper._
import scodec.bits.ByteVector
import scodec.{Codec, codecs}

final case class Events(events: Nel[Event], metadata: PayloadMetadata)

object Events {

  implicit val codecEvents: Codec[Events] = {
    val eventsCodec = nelCodec(codecs.listOfN(codecs.int32, codecs.variableSizeBytes(codecs.int32, Codec[Event])))

    val default = (codecs.ignore(ByteJ.SIZE) ~> eventsCodec)
      .xmap[Events](a => Events(a, PayloadMetadata.empty), _.events)

    val version0 = (codecs.constant(ByteVector.fromByte(0)) ~> eventsCodec)
      .xmap[Events](a => Events(a, PayloadMetadata.empty), _.events)

    val metadataCodec: Codec[PayloadMetadata] = formatCodec[PayloadMetadata]

    val version1 = (codecs.constant(ByteVector.fromByte(1)) ~> (eventsCodec :: metadataCodec))
      .as[Events]

    codecs.choice(version1, version0, default)
  }

  implicit def eventsToBytes[F[_] : FromAttempt]: ToBytes[F, Events] = ToBytes.fromEncoder

  implicit def eventsFromBytes[F[_] : FromAttempt]: FromBytes[F, Events] = FromBytes.fromDecoder
}
