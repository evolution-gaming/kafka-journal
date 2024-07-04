package com.evolutiongaming.kafka.journal

import cats.Functor
import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.conversions.ActionToProducerRecord
import com.evolutiongaming.skafka.consumer.WithSize
import com.evolutiongaming.skafka.{Offset, TimestampAndType, TimestampType, TopicPartition}

object ConsumerRecordOf {

  def apply[F[_]: Functor](action: Action, topicPartition: TopicPartition, offset: Offset)(
    implicit actionToProducerRecord: ActionToProducerRecord[F],
  ): F[ConsRecord] = {

    for {
      producerRecord <- actionToProducerRecord(action)
    } yield {
      val timestampAndType = TimestampAndType(action.timestamp, TimestampType.Create)
      ConsRecord(
        topicPartition   = topicPartition,
        offset           = offset,
        timestampAndType = timestampAndType.some,
        key              = producerRecord.key.map { bytes => WithSize(bytes, bytes.length) },
        value            = producerRecord.value.map { bytes => WithSize(bytes, bytes.length.toInt) },
        headers          = producerRecord.headers,
      )
    }
  }
}
