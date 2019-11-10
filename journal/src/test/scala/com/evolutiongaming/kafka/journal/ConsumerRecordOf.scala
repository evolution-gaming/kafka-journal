package com.evolutiongaming.kafka.journal

import cats.Functor
import cats.implicits._
import com.evolutiongaming.kafka.journal.conversions.ActionToProducerRecord
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, ConsumerRecords, WithSize}
import com.evolutiongaming.skafka.{Offset, TimestampAndType, TimestampType, TopicPartition}
import scodec.bits.ByteVector

object ConsumerRecordOf {

  def apply[F[_] : Functor](
    action: Action,
    topicPartition: TopicPartition,
    offset: Offset)(implicit
    actionToProducerRecord: ActionToProducerRecord[F]
  ): F[ConsumerRecord[String, ByteVector]] = {

    for {
      producerRecord <- actionToProducerRecord(action)
    } yield {
      val timestampAndType = TimestampAndType(action.timestamp, TimestampType.Create)
      ConsumerRecord[String, ByteVector](
        topicPartition = topicPartition,
        offset = offset,
        timestampAndType = Some(timestampAndType),
        key = producerRecord.key.map(bytes => WithSize(bytes, bytes.length)),
        value = producerRecord.value.map(bytes => WithSize(bytes, bytes.length.toInt)),
        headers = producerRecord.headers)
    }
  }
}

object ConsumerRecordsOf {

  def apply[K, V](records: List[ConsumerRecord[K, V]]): ConsumerRecords[K, V] = {
    val records1 = for {
      (topicPartition, records) <- records.groupBy(_.topicPartition)
      records                   <- records.toNel
    } yield {
      (topicPartition, records)
    }
    ConsumerRecords(records1)
  }
}
