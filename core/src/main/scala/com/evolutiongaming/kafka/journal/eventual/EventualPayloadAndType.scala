package com.evolutiongaming.kafka.journal.eventual

import cats.Applicative
import cats.syntax.all._
import com.evolutiongaming.kafka.journal.PayloadType
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.kafka.journal.util.Fail.implicits._
import scodec.bits.ByteVector

final case class EventualPayloadAndType(
  payload: Either[String, ByteVector],
  payloadType: PayloadType
)

object EventualPayloadAndType {

  implicit class EventualPayloadAndTypeOps(val self: EventualPayloadAndType) extends AnyVal {

    def payloadStr[F[_] : Applicative : Fail]: F[String] = self.payload.fold(
      _.pure[F],
      _ => "String expected, but got bytes".fail
    )

    def payloadBytes[F[_] : Applicative : Fail]: F[ByteVector] = self.payload.fold(
      _ => "Bytes expected, but got string".fail,
      _.pure[F]
    )

  }
}
