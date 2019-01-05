package com.evolutiongaming.kafka.journal

import cats.implicits._
import cats.effect.Sync
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal.util.{FromFuture, ToFuture}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.{Offset, Partition, TopicPartition}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration


trait WithPollActions[F[_]] {
  def apply[A](key: Key, partition: Partition, offset: Option[Offset])(f: PollActions[F] => F[A]): F[A]
}

object WithPollActions {

  def async[F[_] : Sync : Log : FromFuture : ToFuture](
    topicConsumer: TopicConsumer[F],
    pollTimeout: FiniteDuration)(implicit ec: ExecutionContext): WithPollActions[Async] = {

    async(apply[F](topicConsumer, pollTimeout))
  }

  def async[F[_] : FromFuture : ToFuture](withPollActions: WithPollActions[F])(implicit ec: ExecutionContext /*TODO remove*/): WithPollActions[Async] = {

    new WithPollActions[Async] {

      def apply[A](key: Key, partition: Partition, offset: Option[Offset])(f: PollActions[Async] => Async[A]) = {

        val ff = (pollActions: PollActions[F]) => {

          val pollActions1 = new PollActions[Async] {
            def apply() = {
              Async {
                ToFuture[F].apply {
                  pollActions()
                }
              }
            }
          }
          
          FromFuture[F].apply {
            f(pollActions1).future
          }
        }

        Async {
          ToFuture[F].apply {
            withPollActions(key, partition, offset)(ff)
          }
        }
      }
    }
  }

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
}