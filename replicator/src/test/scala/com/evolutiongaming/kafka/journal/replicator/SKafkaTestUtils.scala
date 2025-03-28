package com.evolutiongaming.kafka.journal.replicator

import com.evolutiongaming.skafka.{Offset, Partition, TopicPartition}

object SKafkaTestUtils {
  def offset(n: Int): Offset = Offset.unsafe(n)

  def p(n: Int): Partition = Partition.unsafe(n)

  def tp(topic: String, n: Int): TopicPartition = TopicPartition(topic, p(n))
}
