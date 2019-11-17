package com.evolutiongaming.kafka.journal.replicator


import cats.Monad
import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.evolutiongaming.kafka.journal.{ConsRecord, KafkaConsumer}
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer._
import com.evolutiongaming.sstream.Stream
import scodec.bits.ByteVector

import scala.concurrent.duration._


trait TopicConsumer[F[_]] {

  def subscribe(listener: RebalanceListener[F]): F[Unit]

  def poll: Stream[F, Map[Partition, Nel[ConsRecord]]]

  def commit: TopicCommit[F]
}

object TopicConsumer {

  def apply[F[_] : Monad](
    topic: Topic,
    pollTimeout: FiniteDuration,
    commit: TopicCommit[F],
    consumer: KafkaConsumer[F, String, ByteVector],
  ): TopicConsumer[F] = {

    val commit1 = commit

    new TopicConsumer[F] {

      def subscribe(listener: RebalanceListener[F]) = {
        consumer.subscribe(topic, listener.some)
      }

      val poll = {
        val records = for {
          records <- consumer.poll(pollTimeout)
        } yield for {
          (partition, records) <- records.values
        } yield {
          (partition.partition, records)
        }
        Stream.repeat(records)
      }

      def commit = commit1
    }
  }
}
