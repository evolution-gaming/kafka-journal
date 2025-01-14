package com.evolutiongaming.kafka.journal.replicator

import cats.data.*
import com.evolutiongaming.skafka.*
import com.evolutiongaming.skafka.consumer.{ConsumerGroupMetadata, OffsetAndTimestamp, RebalanceConsumer}

import java.time.Instant
import scala.concurrent.duration.*
import scala.util.{Failure, Try}

object EmptyRebalanceConsumer extends RebalanceConsumer {
  override def assignment(): Try[Set[TopicPartition]] = Failure(new NotImplementedError)

  override def beginningOffsets(partitions: NonEmptySet[TopicPartition]): Try[Map[TopicPartition, Offset]] = Failure(
    new NotImplementedError,
  )

  override def beginningOffsets(
      partitions: NonEmptySet[TopicPartition],
      timeout: FiniteDuration,
  ): Try[Map[TopicPartition, Offset]] = Failure(new NotImplementedError)

  override def commit(): Try[Unit] = Failure(new NotImplementedError)

  override def commit(timeout: FiniteDuration): Try[Unit] = Failure(new NotImplementedError)

  override def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): Try[Unit] = Failure(new NotImplementedError)

  override def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata], timeout: FiniteDuration): Try[Unit] = Failure(
    new NotImplementedError,
  )

  override def committed(partitions: NonEmptySet[TopicPartition]): Try[Map[TopicPartition, OffsetAndMetadata]] = Failure(
    new NotImplementedError,
  )

  override def committed(
      partitions: NonEmptySet[TopicPartition],
      timeout: FiniteDuration,
  ): Try[Map[TopicPartition, OffsetAndMetadata]] = Failure(new NotImplementedError)

  override def endOffsets(partitions: NonEmptySet[TopicPartition]): Try[Map[TopicPartition, Offset]] = Failure(
    new NotImplementedError,
  )

  override def endOffsets(partitions: NonEmptySet[TopicPartition], timeout: FiniteDuration): Try[Map[TopicPartition, Offset]] =
    Failure(new NotImplementedError)

  override def groupMetadata(): Try[ConsumerGroupMetadata] = Failure(new NotImplementedError)

  override def topics(): Try[Map[Topic, List[PartitionInfo]]] = Failure(new NotImplementedError)

  override def topics(timeout: FiniteDuration): Try[Map[Topic, List[PartitionInfo]]] = Failure(new NotImplementedError)

  override def offsetsForTimes(
      timestampsToSearch: NonEmptyMap[TopicPartition, Instant],
  ): Try[Map[TopicPartition, Option[OffsetAndTimestamp]]] = Failure(new NotImplementedError)

  override def offsetsForTimes(
      timestampsToSearch: NonEmptyMap[TopicPartition, Instant],
      timeout: FiniteDuration,
  ): Try[Map[TopicPartition, Option[OffsetAndTimestamp]]] = Failure(new NotImplementedError)

  override def partitionsFor(topic: Topic): Try[List[PartitionInfo]] = Failure(new NotImplementedError)

  override def partitionsFor(topic: Topic, timeout: FiniteDuration): Try[List[PartitionInfo]] = Failure(new NotImplementedError)

  override def paused(): Try[Set[TopicPartition]] = Failure(new NotImplementedError)

  override def position(partition: TopicPartition): Try[Offset] = Failure(new NotImplementedError)

  override def position(partition: TopicPartition, timeout: FiniteDuration): Try[Offset] = Failure(new NotImplementedError)

  override def seek(partition: TopicPartition, offset: Offset): Try[Unit] = Failure(new NotImplementedError)

  override def seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata): Try[Unit] = Failure(
    new NotImplementedError,
  )

  override def seekToBeginning(partitions: NonEmptySet[TopicPartition]): Try[Unit] = Failure(new NotImplementedError)

  override def seekToEnd(partitions: NonEmptySet[TopicPartition]): Try[Unit] = Failure(new NotImplementedError)

  override def subscription(): Try[Set[Topic]] = Failure(new NotImplementedError)

}
