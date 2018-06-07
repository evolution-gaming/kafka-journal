package com.evolutiongaming.skafka.concumer

import com.evolutiongaming.skafka._
import scala.collection.immutable.Seq


case class ConsumerRecord[K, V](
  topic: Topic,
  partition: Partition,
  offset: Offset,
  timestampAndType: Option[TimestampAndType],
  serializedKeySize: Int,
  serializedValueSize: Int,
  key: Option[K],
  value: V,
  headers: List[Header])

case class ConsumerRecords[K, V](values: Map[TopicPartition, Seq[ConsumerRecord[K, V]]])
