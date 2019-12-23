package com.evolutiongaming.kafka.journal

import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import java.lang.{Byte => ByteJ}

import cats.Applicative
import com.evolutiongaming.kafka.journal.util.ScodecHelper._
import play.api.libs.json.{Json, OFormat}
import scodec.bits.ByteVector
import scodec.{Codec, codecs}

final case class Events(events: Nel[Event], metadata: Events.Metadata)

object Events {

  implicit val codecEvents: Codec[Events] = {
    val eventsCodec = nelCodec(codecs.listOfN(codecs.int32, codecs.variableSizeBytes(codecs.int32, Codec[Event])))

    val default = (codecs.ignore(ByteJ.SIZE) ~> eventsCodec)
      .xmap[Events](a => Events(a, Metadata.empty), _.events)

    val version0 = (codecs.constant(ByteVector.fromByte(0)) ~> eventsCodec)
      .xmap[Events](a => Events(a, Metadata.empty), _.events)

    codecs.choice(version0, default)
  }

  implicit def eventsToBytes[F[_] : FromAttempt]: ToBytes[F, Events] = ToBytes.fromEncoder

  implicit def eventsFromBytes[F[_] : FromAttempt]: FromBytes[F, Events] = FromBytes.fromDecoder


  final case class Metadata(expireAfter: Option[ExpireAfter])

  object Metadata {

    val empty: Metadata = Metadata(none)

    implicit val formatMetadata: OFormat[Metadata] = Json.format

    implicit def toBytesMetadata[F[_] : Applicative]: ToBytes[F, Metadata] = ToBytes.fromWrites

    implicit def fromBytesMetadata[F[_] : FromJsResult]: FromBytes[F, Metadata] = FromBytes.fromReads
  }
}
