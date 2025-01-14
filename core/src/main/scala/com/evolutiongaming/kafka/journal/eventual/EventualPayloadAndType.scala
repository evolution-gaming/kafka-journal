package com.evolutiongaming.kafka.journal.eventual

import cats.Applicative
import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.PayloadType
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.kafka.journal.util.Fail.implicits.*
import scodec.bits.ByteVector

/** Piece of data prepared for convenient storing into Cassandra row.
  *
  * Usually the data is stored into the structure similar to the following:
  * {{{
  * payload_type TEXT,
  * payload_txt TEXT,
  * payload_bin BLOB
  * }}}
  * Where usage of either `payload_txt` or `payload_bin` column depends on
  * the contents of a `payload` field.
  *
  * The `payloadType` field is used to determine how the contents of `payload`
  * should be treated, i.e. if it should be parsed as JSON.
  */
final case class EventualPayloadAndType(
    payload: Either[String, ByteVector],
    payloadType: PayloadType,
)

object EventualPayloadAndType {

  implicit class EventualPayloadAndTypeOps(val self: EventualPayloadAndType) extends AnyVal {

    def payloadStr[F[_]: Applicative: Fail]: F[String] = self
      .payload
      .fold(
        _.pure[F],
        _ => "String expected, but got bytes".fail,
      )

    def payloadBytes[F[_]: Applicative: Fail]: F[ByteVector] = self
      .payload
      .fold(
        _ => "Bytes expected, but got string".fail,
        _.pure[F],
      )

  }
}
