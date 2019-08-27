package com.evolutiongaming.kafka.journal.util

import cats.Applicative
import cats.implicits._
import com.evolutiongaming.skafka
import com.evolutiongaming.skafka.{Bytes, Topic}
import scodec.bits.ByteVector

object SkafkaHelper {

  implicit def skafkaFromBytes[F[_] : Applicative]: skafka.FromBytes[F, ByteVector] = {
    (a: Bytes, _: Topic) => ByteVector.view(a).pure[F]
  }

  implicit def skafkaToBytes[F[_] : Applicative]: skafka.ToBytes[F, ByteVector] = {
    (a: ByteVector, _: Topic) => a.toArray.pure[F]
  }
}
