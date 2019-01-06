package com.evolutiongaming.kafka.journal

import cats.implicits._
import cats.effect.Sync
import cats.~>
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.{Offset, Partition, TopicPartition}

import scala.concurrent.duration.FiniteDuration


trait WithPollActions[F[_]] {
  def apply[A](key: Key, partition: Partition, offset: Option[Offset])(f: PollActions[F] => F[A]): F[A]
}

object WithPollActions {

  def apply[F[_] : Sync : Log](
    topicConsumer: TopicConsumer[F],
    pollTimeout: FiniteDuration): WithPollActions[F] = {

    new WithPollActions[F] {

      // TODO pass From instead of Last offset
      def apply[A](key: Key, partition: Partition, offset: Option[Offset])(f: PollActions[F] => F[A]) = {
        val consumer = topicConsumer(key.topic)
        val from = offset.fold(Offset.Min)(_ + 1)
        val topicPartition = TopicPartition(topic = key.topic, partition = partition)
        consumer.use { consumer =>
          for {
            _ <- consumer.assign(Nel(topicPartition))
            _ <- consumer.seek(topicPartition, from)
            _ <- Log[F].debug(s"$key consuming from $partition:$from")
            p  = PollActions[F](key, consumer, pollTimeout)
            a <- f(p)
          } yield a
        }
      }
    }
  }


  implicit class WithPollActionsOps[F[_]](val self: WithPollActions[F]) extends AnyVal {

    def mapK[G[_]](to: F ~> G, from: G ~> F): WithPollActions[G] = new WithPollActions[G] {

      def apply[A](key: Key, partition: Partition, offset: Option[Offset])(f: PollActions[G] => G[A]) = {
        val ff = (pollActions: PollActions[F]) => from(f(pollActions.mapK(to)))
        to(self(key, partition, offset)(ff))
      }
    }
  }
}