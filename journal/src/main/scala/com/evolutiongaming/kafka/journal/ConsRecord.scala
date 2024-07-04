package com.evolutiongaming.kafka.journal

import com.evolutiongaming.skafka.consumer.{ConsumerRecord, WithSize}
import com.evolutiongaming.skafka.{Header, Offset, TimestampAndType, TopicPartition}
import scodec.bits.ByteVector

object ConsRecord {

  def apply(
    topicPartition: TopicPartition,
    offset: Offset,
    timestampAndType: Option[TimestampAndType],
    key: Option[WithSize[String]]       = None,
    value: Option[WithSize[ByteVector]] = None,
    headers: List[Header]               = Nil,
  ): ConsRecord = {

    ConsumerRecord(
      topicPartition   = topicPartition,
      offset           = offset,
      timestampAndType = timestampAndType,
      key              = key,
      value            = value,
      headers          = headers,
    )
  }
}
