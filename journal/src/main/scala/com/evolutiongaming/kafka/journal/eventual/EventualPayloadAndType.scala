package com.evolutiongaming.kafka.journal.eventual

import com.evolutiongaming.kafka.journal.PayloadType
import scodec.bits.ByteVector

final case class EventualPayloadAndType(
  payload: Either[String, ByteVector],
  payloadType: PayloadType
)
