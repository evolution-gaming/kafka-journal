package com.evolutiongaming.kafka.journal

import cats.data.{NonEmptyList => Nel, NonEmptyMap => Nem}
import cats.effect._
import cats.implicits._
import cats.{Applicative, Monad, ~>}
import com.evolutiongaming.kafka.journal.util.Named
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer.{Consumer, ConsumerRecords}

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NoStackTrace

trait KafkaConsumer[F[_], K, V] {

  def assign(partitions: Nel[TopicPartition]): F[Unit]

  def seek(partition: TopicPartition, offset: Offset): F[Unit]

  def subscribe(topic: Topic): F[Unit]

  def poll(timeout: FiniteDuration): F[ConsumerRecords[K, V]]

  def commit(offsets: Map[TopicPartition, OffsetAndMetadata]/*TODO*/): F[Unit]

  def topics: F[Set[Topic]]

  def partitions(topic: Topic): F[Set[Partition]]

  def assignment: F[Set[TopicPartition]]
}

object KafkaConsumer {

  def apply[F[_], K, V](implicit F: KafkaConsumer[F, K, V]): KafkaConsumer[F, K, V] = F


  def of[F[_] : Sync, K, V](
    consumer: Resource[F, Consumer[F, K, V]]
  ): Resource[F, KafkaConsumer[F, K, V]] = {

    val result = for {
      result <- consumer.allocated
    } yield {
      val (consumer, close0) = result

      val toError = new Named[F] {
        def apply[A](fa: F[A], method: String) = {
          fa.handleErrorWith { e => KafkaConsumerError(s"consumer.$method", e).raiseError[F, A] }
        }
      }

      val close = toError(close0, "close")

      val kafkaConsumer = apply[F, K, V](consumer).mapMethod(toError)

      (kafkaConsumer, close)
    }
    Resource(result)
  }


  def apply[F[_] : Applicative, K, V](consumer: Consumer[F, K, V]): KafkaConsumer[F, K, V] = {
    new KafkaConsumer[F, K, V] {

      def assign(partitions: Nel[TopicPartition]) = {
        consumer.assign(partitions)
      }

      def seek(partition: TopicPartition, offset: Offset) = {
        consumer.seek(partition, offset)
      }

      def subscribe(topic: Topic) = {
        consumer.subscribe(Nel.of(topic), None)
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


    implicit def withShiftPoll(implicit F: Monad[F], contextShift: ContextShift[F]): KafkaConsumer[F, K, V] = {

      new KafkaConsumer[F, K, V] {

        def assign(partitions: Nel[TopicPartition]) = self.assign(partitions)

        def seek(partition: TopicPartition, offset: Offset) = self.seek(partition, offset)

        def subscribe(topic: Topic) = self.subscribe(topic)

        def poll(timeout: FiniteDuration) = {
          for {
            a <- self.poll(timeout)
            _ <- if (a.values.isEmpty) contextShift.shift else ().pure[F]
          } yield a
        }

        def commit(offsets: Map[TopicPartition, OffsetAndMetadata]) = self.commit(offsets)

        def topics = self.topics

        def partitions(topic: Topic) = self.partitions(topic)

        def assignment = self.assignment
      }
    }
  }
}


final case class KafkaConsumerError(
  message: String,
  cause: Throwable) extends RuntimeException(message, cause) with NoStackTrace