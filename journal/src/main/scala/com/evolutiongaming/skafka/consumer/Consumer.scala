package com.evolutiongaming.skafka.consumer

import java.util.regex.Pattern

import com.evolutiongaming.skafka._

import scala.collection.immutable.Iterable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

trait Consumer[K, V] {

  def assign(partitions: Iterable[TopicPartition]): Unit

  def assignment(): Set[TopicPartition]

  def subscribe(topics: Iterable[Topic], listener: Option[RebalanceListener]): Unit

  def subscribe(pattern: Pattern, listener: Option[RebalanceListener]): Unit

  def subscription(): Set[Topic]

  def unsubscribe(): Future[Unit]

  def poll(timeout: FiniteDuration): Future[ConsumerRecords[K, V]]

  def commit(): Future[Map[TopicPartition, OffsetAndMetadata]]

  def commit(offsets: Map[TopicPartition, OffsetAndMetadata]): Future[Unit]

  def seek(partition: TopicPartition, offset: Offset): Unit

  def seekToBeginning(partitions: Iterable[TopicPartition]): Unit

  def seekToEnd(partitions: Iterable[TopicPartition]): Unit

  def position(partition: TopicPartition): Future[Offset]

  def committed(partition: TopicPartition): Future[OffsetAndMetadata]

  def partitionsFor(topic: Topic): Future[List[PartitionInfo]]

  def listTopics(): Future[Map[Topic, List[PartitionInfo]]]

  def pause(partitions: Iterable[TopicPartition]): Unit

  def paused(): Set[TopicPartition]

  def resume(partitions: Iterable[TopicPartition]): Unit

  def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Offset]): Future[Map[TopicPartition, Option[OffsetAndTimestamp]]]

  def beginningOffsets(partitions: Iterable[TopicPartition]): Future[Map[TopicPartition, Offset]]

  def endOffsets(partitions: Iterable[TopicPartition]): Future[Map[TopicPartition, Offset]]

  def close(): Future[Unit]

  def close(timeout: FiniteDuration): Future[Unit]

  def wakeup(): Future[Unit]
}