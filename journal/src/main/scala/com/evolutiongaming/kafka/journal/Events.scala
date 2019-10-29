package com.evolutiongaming.kafka.journal

import cats.data.{NonEmptyList => Nel}
import com.evolutiongaming.kafka.journal.util.ScodecHelper._
import scodec.bits.ByteVector
import scodec.{Codec, codecs}

final case class Events(events: Nel[Event])

object Events {

  implicit val codecEvents: Codec[Events] = {
    val eventsCodec = nelCodec(codecs.listOfN(codecs.int32, codecs.variableSizeBytes(codecs.int32, Codec[Event])))
    val version = ByteVector.fromByte(0)
    (codecs.constant(version) ~> eventsCodec).as[Events]
  }

  implicit def eventsToBytes[F[_] : FromAttempt]: ToBytes[F, Events] = ToBytes.fromEncoder

  implicit def eventsFromBytes[F[_] : FromAttempt]: FromBytes[F, Events] = FromBytes.fromDecoder
}
