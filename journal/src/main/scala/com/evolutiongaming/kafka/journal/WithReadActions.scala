package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.IO.implicits._
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.consumer.Consumer
import com.evolutiongaming.skafka.{Offset, Partition, TopicPartition}

import scala.compat.Platform
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}


trait WithReadActions[F[_]] {
  def apply[T](key: Key, partition: Partition, offset: Option[Offset])(f: ReadActions[F] => F[T]): F[T]
}

object WithReadActions {

  def apply[F[_] : IO : FromFuture](
    topicConsumer: TopicConsumer,
    pollTimeout: FiniteDuration,
    closeTimeout: FiniteDuration,
    log: ActorLog)(implicit ec: ExecutionContext /*TODO remove*/): WithReadActions[F] = {

    new WithReadActions[F] {

      def apply[A](key: Key, partition: Partition, offset: Option[Offset])(f: ReadActions[F] => F[A]) = {
        // TODO consider separate from splitting
        val consumer = IO[F].effect {
          val timestamp = Platform.currentTime
          val consumer = topicConsumer(key.topic) // TODO ~10ms
          val duration = Platform.currentTime - timestamp
          // TODO add metric
          log.debug(s"$key consumerOf() took $duration ms")
          consumer
        }


        def release(consumer: Consumer[Id, Bytes, Future]) = {
          FromFuture[F].apply {
            for {
              failure <- consumer.close(closeTimeout).failed
            } yield {
              log.error(s"$key failed to close consumer $failure", failure)
            }
          }
        }

        consumer.bracket(release) { consumer =>
          val topicPartition = TopicPartition(topic = key.topic, partition = partition)
          consumer.assign(Nel(topicPartition))

          offset match {
            case None =>
              log.debug(s"$key consuming from $partition:0")
              consumer.seekToBeginning(Nel(topicPartition))

            case Some(offset) =>
              val from = offset + 1
              log.debug(s"$key consuming from $partition:$from")
              consumer.seek(topicPartition, from)
          }

          val readKafka = ReadActions(key, consumer, pollTimeout, log)
          f(readKafka)
        }
      }
    }
  }
}