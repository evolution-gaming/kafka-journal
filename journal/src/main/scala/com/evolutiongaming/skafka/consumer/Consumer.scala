package com.evolutiongaming.skafka.consumer

import java.util.regex.Pattern

import com.evolutiongaming.skafka._

import scala.collection.immutable.Iterable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

trait Consumer[K, V] {

  def assign(partitions: Iterable[TopicPartition]): Unit

  def assignment(): Set[TopicPartition]

  def subscribe(topics: Iterable[Topic]): Unit

  def subscribe(topics: Iterable[Topic], listener: RebalanceListener): Unit

  def subscribe(pattern: Pattern, listener: RebalanceListener): Unit

  def subscribe(pattern: Pattern): Unit

  def subscription(): Set[Topic]

  def unsubscribe(): Unit

  def poll(timeout: FiniteDuration): Future[ConsumerRecords[K, V]]

  def commitSync(): Unit

  def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): Unit

  def commitAsync(): Unit

  def commitAsync(callback: CommitCallback): Unit

  def commitAsync(offsets: Map[TopicPartition, OffsetAndMetadata], callback: CommitCallback): Unit

  def seek(partition: TopicPartition, offset: Offset): Unit

  def seekToBeginning(partitions: Iterable[TopicPartition]): Unit

  def seekToEnd(partitions: Iterable[TopicPartition]): Unit

  def position(partition: TopicPartition): Offset

  def committed(partition: TopicPartition): OffsetAndMetadata

  def partitionsFor(topic: Topic): List[PartitionInfo]

  def listTopics(): Map[Topic, List[PartitionInfo]]

  def pause(partitions: Iterable[TopicPartition]): Unit

  def paused(): Set[TopicPartition]

  def resume(partitions: Iterable[TopicPartition]): Unit

  def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Offset]): Map[TopicPartition, Option[OffsetAndTimestamp]]

  def beginningOffsets(partitions: Iterable[TopicPartition]): Map[TopicPartition, Offset]

  def endOffsets(partitions: Iterable[TopicPartition]): Map[TopicPartition, Offset]

  def close(): Future[Unit]

  def close(timeout: FiniteDuration): Future[Unit]

  def wakeup(): Unit
}