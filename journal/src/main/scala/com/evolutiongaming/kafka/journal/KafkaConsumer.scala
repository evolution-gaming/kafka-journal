package com.evolutiongaming.kafka.journal

import cats.effect.{ContextShift, Sync}
import cats.implicits._
import com.evolutiongaming.kafka.journal.util.FromFuture
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer.{Consumer, ConsumerConfig, ConsumerRecords}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

trait KafkaConsumer[F[_]] {

  def assign(partitions: Nel[TopicPartition]): F[Unit]

  def seek(partition: TopicPartition, offset: Offset): F[Unit]

  def subscribe(topic: Topic): F[Unit]

  def poll(timeout: FiniteDuration): F[ConsumerRecords[String, Bytes]]

  def commit(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit]

  def topics: F[Set[Topic]]

  def partitions(topic: Topic): F[List[Partition]]

  def close: F[Unit]
}

object KafkaConsumer {

  def apply[F[_]](implicit F: KafkaConsumer[F]): KafkaConsumer[F] = F

  def of[F[_] : Sync : FromFuture : ContextShift](
    config: ConsumerConfig,
    blocking: ExecutionContext,
    metrics: Option[Consumer.Metrics] = None): F[KafkaConsumer[F]] = {

    for {
      consumer0 <- ContextShift[F].evalOn(blocking) {
        Sync[F].delay { Consumer[Id, Bytes](config, blocking) }
      }
    } yield {
      val consumer = metrics.fold(consumer0) { metrics => Consumer(consumer0, metrics) }

      new KafkaConsumer[F] {

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
            infos <- FromFuture[F].apply {
              consumer.partitions(topic)
            }
          } yield for {
            info <- infos
          } yield {
            info.partition
          }
        }

        def close = FromFuture[F].apply { consumer.close() }
      }
    }
  }
}
