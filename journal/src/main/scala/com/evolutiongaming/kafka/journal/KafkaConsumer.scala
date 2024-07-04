package com.evolutiongaming.kafka.journal

import cats.data.{NonEmptyMap as Nem, NonEmptySet as Nes}
import cats.effect.*
import cats.syntax.all.*
import cats.{Applicative, ~>}
import com.evolutiongaming.kafka.journal.util.Named
import com.evolutiongaming.skafka.*
import com.evolutiongaming.skafka.consumer.{Consumer, ConsumerRecords, RebalanceListener}

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NoStackTrace

trait KafkaConsumer[F[_], K, V] {

  def assign(partitions: Nes[TopicPartition]): F[Unit]

  def seek(partition: TopicPartition, offset: Offset): F[Unit]

  def subscribe(topic: Topic, listener: Option[RebalanceListener[F]]): F[Unit]

  def poll(timeout: FiniteDuration): F[ConsumerRecords[K, V]]

  def commit(offsets: Nem[TopicPartition, OffsetAndMetadata]): F[Unit]

  def topics: F[Set[Topic]]

  def partitions(topic: Topic): F[Set[Partition]]

  def assignment: F[Set[TopicPartition]]
}

object KafkaConsumer {

  def apply[F[_], K, V](implicit F: KafkaConsumer[F, K, V]): KafkaConsumer[F, K, V] = F

  def of[F[_]: Sync, K, V](
    consumer: Resource[F, Consumer[F, K, V]],
  ): Resource[F, KafkaConsumer[F, K, V]] = {

    val result = for {
      result <- consumer.allocated
    } yield {
      val (consumer, close0) = result

      val toError = new Named[F] {
        def apply[A](fa: F[A], method: String) = {
          fa.adaptError { case e => KafkaConsumerError(s"consumer.$method", e) }
        }
      }

      val close = toError(close0, "close")

      val kafkaConsumer = apply[F, K, V](consumer).mapMethod(toError)

      (kafkaConsumer, close)
    }
    Resource(result)
  }

  def apply[F[_]: Applicative, K, V](consumer: Consumer[F, K, V]): KafkaConsumer[F, K, V] = {
    class Main
    new Main with KafkaConsumer[F, K, V] {

      def assign(partitions: Nes[TopicPartition]) = {
        consumer.assign(partitions)
      }

      def seek(partition: TopicPartition, offset: Offset) = {
        consumer.seek(partition, offset)
      }

      def subscribe(topic: Topic, listener: Option[RebalanceListener[F]]) = {
        consumer.subscribe(Nes.of(topic), listener)
      }

      def poll(timeout: FiniteDuration) = {
        consumer.poll(timeout)
      }

      def commit(offsets: Nem[TopicPartition, OffsetAndMetadata]) = {
        consumer.commit(offsets)
      }

      def topics: F[Set[Topic]] = {
        consumer
          .topics
          .map { _.keySet }
      }

      def partitions(topic: Topic) = {
        consumer
          .partitions(topic)
          .map { infos =>
            infos.map { _.partition }.toSet
          }
      }

      def assignment = {
        consumer.assignment
      }
    }
  }

  private sealed abstract class MapK

  private sealed abstract class MapMethod

  private sealed abstract class ShiftPoll

  implicit class KafkaConsumerOps[F[_], K, V](val self: KafkaConsumer[F, K, V]) extends AnyVal {

    def mapK[G[_]](fg: F ~> G, gf: G ~> F): KafkaConsumer[G, K, V] = new MapK with KafkaConsumer[G, K, V] {

      def assign(partitions: Nes[TopicPartition]) = fg(self.assign(partitions))

      def seek(partition: TopicPartition, offset: Offset) = fg(self.seek(partition, offset))

      def subscribe(topic: Topic, listener: Option[RebalanceListener[G]]) = {
        val listener1 = listener.map(_.mapK(gf))
        fg(self.subscribe(topic, listener1))
      }

      def poll(timeout: FiniteDuration) = fg(self.poll(timeout))

      def commit(offsets: Nem[TopicPartition, OffsetAndMetadata]) = fg(self.commit(offsets))

      def topics = fg(self.topics)

      def partitions(topic: Topic) = fg(self.partitions(topic))

      def assignment = fg(self.assignment)
    }

    def mapMethod(f: Named[F]): KafkaConsumer[F, K, V] = new MapMethod with KafkaConsumer[F, K, V] {

      def assign(partitions: Nes[TopicPartition]) = f(self.assign(partitions), "assign")

      def seek(partition: TopicPartition, offset: Offset) = f(self.seek(partition, offset), "seek")

      def subscribe(topic: Topic, listener: Option[RebalanceListener[F]]) = {
        f(self.subscribe(topic, listener), "subscribe")
      }

      def poll(timeout: FiniteDuration) = f(self.poll(timeout), "poll")

      def commit(offsets: Nem[TopicPartition, OffsetAndMetadata]) = f(self.commit(offsets), "commit")

      def topics = f(self.topics, "topics")

      def partitions(topic: Topic) = f(self.partitions(topic), "partitions")

      def assignment = f(self.assignment, "assignment")
    }

    def shiftPoll(implicit F: Temporal[F]): KafkaConsumer[F, K, V] = {

      new ShiftPoll with KafkaConsumer[F, K, V] {

        def assign(partitions: Nes[TopicPartition]) = self.assign(partitions)

        def seek(partition: TopicPartition, offset: Offset) = self.seek(partition, offset)

        def subscribe(topic: Topic, listener: Option[RebalanceListener[F]]) = {
          self.subscribe(topic, listener)
        }

        def poll(timeout: FiniteDuration) = {
          for {
            a <- self.poll(timeout)
            _ <- F.cede
          } yield a
        }

        def commit(offsets: Nem[TopicPartition, OffsetAndMetadata]) = self.commit(offsets)

        def topics = self.topics

        def partitions(topic: Topic) = self.partitions(topic)

        def assignment = self.assignment
      }
    }
  }
}

final case class KafkaConsumerError(message: String, cause: Throwable) extends RuntimeException(message, cause) with NoStackTrace
