package com.evolutiongaming.skafka.consumer

import java.lang.{Long => LongJ}
import java.util.regex.Pattern

import com.evolutiongaming.concurrent.FutureHelper._
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer.ConsumerConverters._
import org.apache.kafka.clients.consumer.{KafkaConsumer, Consumer => ConsumerJ}

import scala.collection.JavaConverters._
import scala.collection.immutable.Iterable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try
import scala.util.control.NonFatal

object CreateConsumer {

  def apply[K, V](
    config: ConsumerConfig,
    ecBlocking: ExecutionContext)(implicit
    valueFromBytes: FromBytes[V],
    keyFromBytes: FromBytes[K]): Consumer[K, V] = {

    val valueDeserializer = valueFromBytes.asJava
    val keyDeserializer = keyFromBytes.asJava
    val consumer = new KafkaConsumer(config.properties, keyDeserializer, valueDeserializer)
    apply(consumer, ecBlocking)
  }

  def apply[K, V](consumer: ConsumerJ[K, V], ecBlocking: ExecutionContext): Consumer[K, V] = {

    def blocking[T](f: => T): Future[T] = Future(f)(ecBlocking)

    def callbackAndFuture() = {
      val promise = Promise[Map[TopicPartition, OffsetAndMetadata]]()
      val callback = new CommitCallback {
        def apply(offsets: Try[Map[TopicPartition, OffsetAndMetadata]]) = {
          promise.complete(offsets)
        }
      }
      (callback, promise.future)
    }

    new Consumer[K, V] {

      def assign(partitions: Iterable[TopicPartition]) = {
        val partitionsJ = partitions.map(_.asJava).asJavaCollection
        consumer.assign(partitionsJ)
      }

      def assignment(): Set[TopicPartition] = {
        val partitionsJ = consumer.assignment()
        partitionsJ.asScala.map(_.asScala).toSet
      }

      def subscribe(topics: Iterable[Topic], listener: Option[RebalanceListener]) = {
        val topicsJ = topics.asJavaCollection
        consumer.subscribe(topicsJ, (listener getOrElse RebalanceListener.Empty).asJava)
      }

      def subscribe(pattern: Pattern, listener: Option[RebalanceListener]) = {
        consumer.subscribe(pattern, (listener getOrElse RebalanceListener.Empty).asJava)
      }

      def subscription() = {
        consumer.subscription().asScala.toSet
      }

      def unsubscribe() = {
        blocking {
          consumer.unsubscribe()
        }
      }

      def poll(timeout: FiniteDuration): Future[ConsumerRecords[K, V]] = {
        blocking {
          val records = consumer.poll(timeout.toMillis)
          records.asScala
        }
      }

      def commit() = {
        try {
          val (callback, future) = callbackAndFuture()
          consumer.commitAsync(callback.asJava)
          future
        } catch {
          case NonFatal(failure) => Future.failed(failure)
        }
      }

      def commit(offsets: Map[TopicPartition, OffsetAndMetadata]) = {
        try {
          val (callback, future) = callbackAndFuture()
          val offsetsJ = offsets.asJavaMap(_.asJava, _.asJava)
          consumer.commitAsync(offsetsJ, callback.asJava)
          future.unit
        } catch {
          case NonFatal(failure) => Future.failed(failure)
        }
      }

      def seek(partition: TopicPartition, offset: Offset) = {
        consumer.seek(partition.asJava, offset)
      }

      def seekToBeginning(partitions: Iterable[TopicPartition]) = {
        val partitionsJ = partitions.map(_.asJava).asJavaCollection
        consumer.seekToBeginning(partitionsJ)
      }

      def seekToEnd(partitions: Iterable[TopicPartition]) = {
        val partitionsJ = partitions.map(_.asJava).asJavaCollection
        consumer.seekToEnd(partitionsJ)
      }

      def position(partition: TopicPartition) = {
        blocking {
          consumer.position(partition.asJava)
        }
      }

      def committed(partition: TopicPartition) = {
        val partitionJ = partition.asJava
        blocking {
          val offsetAndMetadataJ = consumer.committed(partitionJ)
          offsetAndMetadataJ.asScala
        }
      }

      def partitionsFor(topic: Topic) = {
        blocking {
          val partitionInfosJ = consumer.partitionsFor(topic)
          partitionInfosJ.asScala.map(_.asScala).toList
        }
      }

      def listTopics() = {
        blocking {
          val result = consumer.listTopics()
          result.asScalaMap(k => k, _.asScala.map(_.asScala).toList)
        }
      }

      def pause(partitions: Iterable[TopicPartition]) = {
        val partitionsJ = partitions.map(_.asJava).asJavaCollection
        consumer.pause(partitionsJ)
      }

      def paused(): Set[TopicPartition] = {
        val partitionsJ = consumer.paused()
        partitionsJ.asScala.map(_.asScala).toSet
      }

      def resume(partitions: Iterable[TopicPartition]) = {
        val partitionsJ = partitions.map(_.asJava).asJavaCollection
        consumer.resume(partitionsJ)
      }

      def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Offset]) = {
        blocking {
          val timestampsToSearchJ = timestampsToSearch.asJavaMap(_.asJava, LongJ.valueOf)
          val result = consumer.offsetsForTimes(timestampsToSearchJ)
          result.asScalaMap(_.asScala, v => Option(v).map(_.asScala))
        }
      }

      def beginningOffsets(partitions: Iterable[TopicPartition]) = {
        blocking {
          val partitionsJ = partitions.map(_.asJava).asJavaCollection
          val result = consumer.beginningOffsets(partitionsJ)
          result.asScalaMap(_.asScala, v => v)
        }
      }

      def endOffsets(partitions: Iterable[TopicPartition]) = {
        blocking {
          val partitionsJ = partitions.map(_.asJava).asJavaCollection
          val result = consumer.endOffsets(partitionsJ)
          result.asScalaMap(_.asScala, v => v)
        }
      }

      def close() = {
        blocking {
          consumer.close()
        }
      }

      def close(timeout: FiniteDuration) = {
        blocking {
          consumer.close(timeout.length, timeout.unit)
        }
      }

      def wakeup() = {
        blocking {
          consumer.wakeup()
        }
      }
    }
  }
}

