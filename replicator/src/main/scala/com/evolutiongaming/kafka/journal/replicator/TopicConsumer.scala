package com.evolutiongaming.kafka.journal.replicator


import cats.Monad
import cats.implicits._
import com.evolutiongaming.kafka.journal.{ConsRecords, KafkaConsumer}
import com.evolutiongaming.skafka.consumer.{Consumer => _, _}
import com.evolutiongaming.skafka._
import com.evolutiongaming.sstream.Stream
import scodec.bits.ByteVector

import scala.concurrent.duration._


trait TopicConsumer[F[_]] {

  def subscribe(listener: RebalanceListener[F]): F[Unit]

  // TODO return same topic values
  def poll: Stream[F, ConsRecords]

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

      val poll = Stream.repeat(consumer.poll(pollTimeout))

      def commit = commit1
    }
  }
}
