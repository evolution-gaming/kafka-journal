package com.evolutiongaming.skafka.concumer

import java.io.Closeable
import java.lang.{Long => LongJ}
import java.util.regex.Pattern

import com.evolutiongaming.skafka.Converters2._
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.concumer.ConsumerConverters._
import org.apache.kafka.clients.consumer.{Consumer => ConsumerJ}

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

trait Consumer[K, V] extends Closeable {

  def assign(partitions: Iterable[TopicPartition]): Unit

  def assignment(): Set[TopicPartition]

  def subscribe(topics: Iterable[Topic]): Unit

  def subscribe(topics: Iterable[Topic], listener: RebalanceListener): Unit

  def subscribe(pattern: Pattern, listener: RebalanceListener): Unit

  def subscribe(pattern: Pattern): Unit

  def subscription(): Set[Topic]

  def unsubscribe(): Unit

  def poll(timeout: FiniteDuration): ConsumerRecords[K, V]

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

  def close(): Unit

  def close(timeout: FiniteDuration): Unit

  def wakeup(): Unit
}

object Consumer {

  def apply[K, V](consumer: ConsumerJ[K, V]): Consumer[K, V] = new Consumer[K, V] {

    def assign(partitions: Iterable[TopicPartition]): Unit = {
      val partitionsJ = partitions.map(_.asJava).asJavaCollection
      consumer.assign(partitionsJ)
    }

    def assignment(): Set[TopicPartition] = {
      val partitionsJ = consumer.assignment()
      partitionsJ.asScala.map(_.asScala).toSet
    }

    def subscribe(topics: Iterable[Topic]): Unit = {
      val topicsJ = topics.asJavaCollection
      consumer.subscribe(topicsJ)
    }

    def subscribe(topics: Iterable[Topic], listener: RebalanceListener): Unit = {
      val topicsJ = topics.asJavaCollection
      consumer.subscribe(topicsJ, listener.asJava)
    }

    def subscribe(pattern: Pattern, listener: RebalanceListener): Unit = {
      consumer.subscribe(pattern, listener.asJava)
    }

    def subscribe(pattern: Pattern): Unit = {
      consumer.subscribe(pattern)
    }

    def subscription(): Set[Topic] = {
      consumer.subscription().asScala.toSet
    }

    def unsubscribe(): Unit = {
      consumer.unsubscribe()
    }

    def poll(timeout: FiniteDuration): ConsumerRecords[K, V] = {
      val records = consumer.poll(timeout.toMillis)
      records.asScala
    }

    def commitSync(): Unit = {
      consumer.commitSync()
    }

    def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): Unit = {
      val offsetsJ = offsets.asJavaMap(_.asJava, _.asJava)
      consumer.commitSync(offsetsJ)
    }

    def commitAsync(): Unit = {
      consumer.commitAsync()
    }

    def commitAsync(callback: CommitCallback): Unit = {
      consumer.commitAsync(callback.asJava)
    }

    def commitAsync(offsets: Map[TopicPartition, OffsetAndMetadata], callback: CommitCallback): Unit = {
      val offsetsJ = offsets.asJavaMap(_.asJava, _.asJava)
      consumer.commitAsync(offsetsJ, callback.asJava)
    }

    def seek(partition: TopicPartition, offset: Offset): Unit = {
      consumer.seek(partition.asJava, offset)
    }

    def seekToBeginning(partitions: Iterable[TopicPartition]): Unit = {
      val partitionsJ = partitions.map(_.asJava).asJavaCollection
      consumer.seekToBeginning(partitionsJ)
    }

    def seekToEnd(partitions: Iterable[TopicPartition]): Unit = {
      val partitionsJ = partitions.map(_.asJava).asJavaCollection
      consumer.seekToEnd(partitionsJ)
    }

    def position(partition: TopicPartition): Offset = {
      consumer.position(partition.asJava)
    }

    def committed(partition: TopicPartition): OffsetAndMetadata = {
      val partitionJ = partition.asJava
      val offsetAndMetadataJ = consumer.committed(partitionJ)
      offsetAndMetadataJ.asScala
    }

    def partitionsFor(topic: Topic): List[PartitionInfo] = {
      val partitionInfosJ = consumer.partitionsFor(topic)
      partitionInfosJ.asScala.map(_.asScala).toList
    }

    def listTopics(): Map[Topic, List[PartitionInfo]] = {
      val result = consumer.listTopics()
      result.asScalaMap(k => k, _.asScala.map(_.asScala).toList)
    }

    def pause(partitions: Iterable[TopicPartition]): Unit = {
      val partitionsJ = partitions.map(_.asJava).asJavaCollection
      consumer.pause(partitionsJ)
    }

    def paused(): Set[TopicPartition] = {
      val partitionsJ = consumer.paused()
      partitionsJ.asScala.map(_.asScala).toSet
    }

    def resume(partitions: Iterable[TopicPartition]): Unit = {
      val partitionsJ = partitions.map(_.asJava).asJavaCollection
      consumer.resume(partitionsJ)
    }

    def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Offset]): Map[TopicPartition, Option[OffsetAndTimestamp]] = {
      val timestampsToSearchJ = timestampsToSearch.asJavaMap(_.asJava, LongJ.valueOf)
      val result = consumer.offsetsForTimes(timestampsToSearchJ)
      result.asScalaMap(_.asScala, v => Option(v).map(_.asScala))
    }

    def beginningOffsets(partitions: Iterable[TopicPartition]): Map[TopicPartition, Offset] = {
      val partitionsJ = partitions.map(_.asJava).asJavaCollection
      val result = consumer.beginningOffsets(partitionsJ)
      result.asScalaMap(_.asScala, v => v)
    }

    def endOffsets(partitions: Iterable[TopicPartition]): Map[TopicPartition, Offset] = {
      val partitionsJ = partitions.map(_.asJava).asJavaCollection
      val result = consumer.endOffsets(partitionsJ)
      result.asScalaMap(_.asScala, v => v)
    }

    def close() = consumer.close()

    def close(timeout: FiniteDuration) = consumer.close(timeout.length, timeout.unit)

    def wakeup() = consumer.wakeup()
  }
}
