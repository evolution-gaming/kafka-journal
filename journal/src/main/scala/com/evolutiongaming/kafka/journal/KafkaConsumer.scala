package com.evolutiongaming.kafka.journal

import cats.effect.concurrent.Semaphore
import cats.effect.implicits._
import cats.effect.{Concurrent, ContextShift, Resource, Sync}
import cats.implicits._
import cats.~>
import com.evolutiongaming.kafka.journal.util.{FromFuture, Named}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer.{Consumer, ConsumerConfig, ConsumerRecords}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NoStackTrace

// TODO create by Config factories
trait KafkaConsumer[F[_], K, V] {

  def assign(partitions: Nel[TopicPartition]): F[Unit]

  def seek(partition: TopicPartition, offset: Offset): F[Unit]

  def subscribe(topic: Topic): F[Unit]

  def poll(timeout: FiniteDuration): F[ConsumerRecords[K, V]]

  def commit(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit]

  def topics: F[Set[Topic]]

  def partitions(topic: Topic): F[List[Partition]]
}

object KafkaConsumer {

  def apply[F[_], K, V](implicit F: KafkaConsumer[F, K, V]): KafkaConsumer[F, K, V] = F

  def of[F[_] : Concurrent : FromFuture : ContextShift, K: skafka.FromBytes, V: skafka.FromBytes](
    config: ConsumerConfig,
    blocking: ExecutionContext,
    metrics: Option[Consumer.Metrics] = None): Resource[F, KafkaConsumer[F, K, V]] = {

    val result = for {
      semaphore <- Semaphore[F](1)
      consumer0 <- ContextShift[F].evalOn(blocking) { Sync[F].delay { Consumer[K, V](config, blocking) } }
    } yield {
      val consumer = metrics.fold(consumer0) { metrics => Consumer(consumer0, metrics) }

      val serial = new (F ~> F) {
        def apply[A](fa: F[A]) = semaphore.withPermit(fa).uncancelable
      }

      val toError = new Named[F] {
        def apply[A](fa: F[A], method: String) = {
          fa.recoverWith { case e => KafkaConsumerError(s"consumer.$method", e).raiseError[F, A] }
        }
      }

      val close = {
        val close = FromFuture[F].apply { consumer.close() }
        toError(serial(close), "close")
      }

      val kafkaConsumer = apply[F, K, V](consumer)
        .mapK(serial)
        .mapMethod(toError)

      (kafkaConsumer, close)
    }
    Resource(result)
  }


  def apply[F[_] : Sync : FromFuture, K, V](consumer: Consumer[K, V, Future]): KafkaConsumer[F, K, V] = {
    new KafkaConsumer[F, K, V] {

      def assign(partitions: Nel[TopicPartition]) = {
        Sync[F].delay {
          consumer.assign(partitions)
        }
      }

      def seek(partition: TopicPartition, offset: Offset) = {
        Sync[F].delay {
          consumer.seek(partition, offset)
        }
      }

      def subscribe(topic: Topic) = {
        Sync[F].delay {
          consumer.subscribe(Nel(topic), None)
        }
      }

      def poll(timeout: FiniteDuration) = {
        FromFuture[F].apply {
          consumer.poll(timeout)
        }
      }

      def commit(offsets: Map[TopicPartition, OffsetAndMetadata]) = {
        FromFuture[F].apply {
          consumer.commit(offsets)
        }
      }

      def topics: F[Set[Topic]] = {
        for {
          infos <- FromFuture[F].apply { consumer.listTopics() }
        } yield {
          infos.keySet
        }
      }

      def partitions(topic: Topic) = {
        for {
          infos <- FromFuture[F].apply { consumer.partitions(topic) }
        } yield for {
          info <- infos
        } yield {
          info.partition
        }
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
    }


    def mapMethod(f: Named[F]): KafkaConsumer[F, K, V] = new KafkaConsumer[F, K, V] {

      def assign(partitions: Nel[TopicPartition]) = f(self.assign(partitions), "assign")

      def seek(partition: TopicPartition, offset: Offset) = f(self.seek(partition, offset), "seek")

      def subscribe(topic: Topic) = f(self.subscribe(topic), "subscribe")

      def poll(timeout: FiniteDuration) = f(self.poll(timeout), "poll")

      def commit(offsets: Map[TopicPartition, OffsetAndMetadata]) = f(self.commit(offsets), "commit")

      def topics = f(self.topics, "topics")

      def partitions(topic: Topic) = f(self.partitions(topic), "partitions")
    }
  }
}


final case class KafkaConsumerError(message: String, cause: Throwable) extends RuntimeException with NoStackTrace