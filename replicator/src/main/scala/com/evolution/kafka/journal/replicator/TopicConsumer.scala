package com.evolution.kafka.journal.replicator

import cats.Monad
import cats.data.{NonEmptyList as Nel, NonEmptySet as Nes}
import cats.effect.Resource
import cats.syntax.all.*
import com.evolution.kafka.journal.{ConsRecord, KafkaConsumer}
import com.evolutiongaming.skafka.*
import com.evolutiongaming.skafka.consumer.RebalanceListener1
import com.evolutiongaming.sstream.Stream
import scodec.bits.ByteVector

import scala.concurrent.duration.*

private[journal] trait TopicConsumer[F[_]] {

  def subscribe(topics: Nes[Topic], listener: RebalanceListener1[F]): F[Unit]

  def poll: Stream[F, Map[TopicPartition, Nel[ConsRecord]]]

  def commitOf(topic: Topic): Resource[F, TopicCommit[F]]
}

private[journal] object TopicConsumer {

  def apply[F[_]: Monad](
    pollTimeout: FiniteDuration,
    consumer: KafkaConsumer[F, String, ByteVector],
    commitOf: Topic => Resource[F, TopicCommit[F]],
  ): TopicConsumer[F] = {

    val commitOf1 = commitOf

    new TopicConsumer[F] {

      def subscribe(topics: Nes[Topic], listener: RebalanceListener1[F]): F[Unit] = {
        consumer.subscribe(topics, listener)
      }

      val poll: Stream[F, Map[TopicPartition, Nel[ConsRecord]]] = {
        Stream.repeat(consumer.poll(pollTimeout).map { _.values })
      }

      def commitOf(topic: Topic): Resource[F, TopicCommit[F]] = commitOf1(topic)
    }
  }
}
