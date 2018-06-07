package com.evolutiongaming.skafka.concumer

import com.evolutiongaming.skafka.TopicPartition

import scala.util.Try

trait CommitCallback {
  def apply(offsets: Try[Map[TopicPartition, OffsetAndMetadata]]): Unit
}
