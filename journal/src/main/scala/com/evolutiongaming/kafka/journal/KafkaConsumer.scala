package com.evolutiongaming.kafka.journal

import cats.effect.concurrent.Semaphore
import cats.effect.implicits._
import cats.effect._
import cats.implicits._
import cats.~>
import com.evolutiongaming.kafka.journal.util.Named
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer.{Consumer, ConsumerConfig, ConsumerMetrics, ConsumerRecords}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NoStackTrace

trait KafkaConsumer[F[_], K, V] {

  def assign(partitions: Nel[TopicPartition]): F[Unit]

  def seek(partition: TopicPartition, offset: Offset): F[Unit]

  def subscribe(topic: Topic): F[Unit]

  def poll(timeout: FiniteDuration): F[ConsumerRecords[K, V]]

  def commit(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit]

  def topics: F[Set[Topic]]

  def partitions(topic: Topic): F[Set[Partition]]

  def assignment: F[Set[TopicPartition]]
}

object KafkaConsumer {

  def apply[F[_], K, V](implicit F: KafkaConsumer[F, K, V]): KafkaConsumer[F, K, V] = F

  def of[F[_] : Concurrent : ContextShift : Clock, K: skafka.FromBytes, V: skafka.FromBytes](
    config: ConsumerConfig,
    blocking: ExecutionContext,
    metrics: Option[ConsumerMetrics[F]] = None
  ): Resource[F, KafkaConsumer[F, K, V]] = {

    val result = for {
      semaphore <- Semaphore[F](1)
      ab        <- Consumer.of[F, K, V](config, blocking).allocated
    } yield {
      val (consumer0, close0) = ab
      val consumer = metrics.fold(consumer0)(consumer0.withMetrics(_))

      val serial = new (F ~> F) {
        def apply[A](fa: F[A]) = semaphore.withPermit(fa).uncancelable
      }

      val toError = new Named[F] {
        def apply[A](fa: F[A], method: String) = {
          fa.handleErrorWith { e => KafkaConsumerError(s"consumer.$method", e).raiseError[F, A] }
        }
      }

      val close = toError(serial(close0), "close")

      val kafkaConsumer = apply[F, K, V](consumer)
        .mapK(serial)
        .mapMethod(toError)

      (kafkaConsumer, close)
    }
    Resource(result)
  }


  def apply[F[_] : Sync, K, V](consumer: Consumer[F, K, V]): KafkaConsumer[F, K, V] = {
    new KafkaConsumer[F, K, V] {

      def assign(partitions: Nel[TopicPartition]) = {
        consumer.assign(partitions)
      }

      def seek(partition: TopicPartition, offset: Offset) = {
        consumer.seek(partition, offset)
      }

      def subscribe(topic: Topic) = {
        consumer.subscribe(Nel(topic), None)
      }

      def poll(timeout: FiniteDuration) = {
        consumer.poll(timeout)
      }

      def commit(offsets: Map[TopicPartition, OffsetAndMetadata]) = {
        consumer.commit(offsets)
      }

      def topics: F[Set[Topic]] = {
        for {
          infos <- consumer.listTopics
        } yield {
          infos.keySet
        }
      }

      def partitions(topic: Topic) = {
        for {
          infos <- consumer.partitions(topic)
        } yield for {
          info <- infos.to[Set]
        } yield {
          info.partition
        }
      }

      def assignment = {
        consumer.assignment
      }
    }
  }


  implicit class KafkaConsumerOps[F[_], K, V](val self: KafkaConsumer[F, K, V]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): KafkaConsumer[G, K, V] = new KafkaConsumer[G, K, V] {

      def assign(partitions: Nel[TopicPartition]) = f(self.assign(partitions))

      def seek(partition: TopicPartition, offset: Offset) = f(self.seek(partition, offset))

      def subscribe(topic: Topic) = f(self.subscribe(topic))

      def poll(timeout: FiniteDuration) = f(self.poll(timeout))

      def commit(offsets: Map[TopicPartition, OffsetAndMetadata]) = f(self.commit(offsets))

      def topics = f(self.topics)

      def partitions(topic: Topic) = f(self.partitions(topic))

      def assignment = f(self.assignment)
    }


    def mapMethod(f: Named[F]): KafkaConsumer[F, K, V] = new KafkaConsumer[F, K, V] {

      def assign(partitions: Nel[TopicPartition]) = f(self.assign(partitions), "assign")

      def seek(partition: TopicPartition, offset: Offset) = f(self.seek(partition, offset), "seek")

      def subscribe(topic: Topic) = f(self.subscribe(topic), "subscribe")

      def poll(timeout: FiniteDuration) = f(self.poll(timeout), "poll")

      def commit(offsets: Map[TopicPartition, OffsetAndMetadata]) = f(self.commit(offsets), "commit")

      def topics = f(self.topics, "topics")

      def partitions(topic: Topic) = f(self.partitions(topic), "partitions")

      def assignment = f(self.assignment, "assignment")
    }
  }
}


final case class KafkaConsumerError(
  message: String,
  cause: Throwable) extends RuntimeException(message, cause) with NoStackTrace