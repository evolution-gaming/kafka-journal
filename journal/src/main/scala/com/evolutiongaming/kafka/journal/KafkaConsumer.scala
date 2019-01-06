package com.evolutiongaming.kafka.journal

import cats.effect.concurrent.Semaphore
import cats.effect.implicits._
import cats.effect.{Concurrent, ContextShift, Resource, Sync}
import cats.implicits._
import com.evolutiongaming.kafka.journal.util.FromFuture
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer.{Consumer, ConsumerConfig, ConsumerRecords}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

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

    Resource {
      for {
        semaphore <- Semaphore[F](1)
        consumer0 <- ContextShift[F].evalOn(blocking) {
          Sync[F].delay { Consumer[K, V](config, blocking) }
        }
      } yield {
        val consumer = metrics.fold(consumer0) { metrics => Consumer(consumer0, metrics) }

        def serial[A](fa: F[A]) = semaphore.withPermit(fa).uncancelable

        val release = serial {
          FromFuture[F].apply { consumer.close() }
        }

        val result = new KafkaConsumer[F, K, V] {

          def assign(partitions: Nel[TopicPartition]) = {
            serial {
              Sync[F].delay {
                consumer.assign(partitions)
              }
            }
          }

          def seek(partition: TopicPartition, offset: Offset) = {
            serial {
              Sync[F].delay {
                consumer.seek(partition, offset)
              }
            }
          }

          def subscribe(topic: Topic) = {
            serial {
              Sync[F].delay {
                consumer.subscribe(Nel(topic), None)
              }
            }
          }

          def poll(timeout: FiniteDuration) = {
            serial {
              FromFuture[F].apply {
                consumer.poll(timeout)
              }
            }
          }

          def commit(offsets: Map[TopicPartition, OffsetAndMetadata]) = {
            serial {
              FromFuture[F].apply {
                consumer.commit(offsets)
              }
            }
          }

          def topics: F[Set[Topic]] = {
            serial {
              for {
                infos <- FromFuture[F].apply { consumer.listTopics() }
              } yield {
                infos.keySet
              }
            }
          }

          def partitions(topic: Topic) = {
            for {
              infos <- serial {
                FromFuture[F].apply {
                  consumer.partitions(topic)
                }
              }
            } yield for {
              info <- infos
            } yield {
              info.partition
            }
          }
        }

        (result, release)
      }
    }
  }
}
