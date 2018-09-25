package com.evolutiongaming.kafka.journal

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.consumer.Consumer
import com.evolutiongaming.skafka.{Offset, Partition, Topic, TopicPartition}

import scala.compat.Platform
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration


// TODO pass partition even if offset is unknown
trait WithReadActions[F[_]] {
  def apply[T](key: Key, partition: Partition, offset: Option[Offset])(f: ReadActions[F] => F[T]): F[T]
}

object WithReadActions {

  def apply(
    consumerOf: Topic => Consumer[Id, Bytes],
    pollTimeout: FiniteDuration,
    closeTimeout: FiniteDuration,
    log: ActorLog)(implicit ec: ExecutionContext /*TODO remove*/): WithReadActions[Async] = {

    new WithReadActions[Async] {

      def apply[T](key: Key, partition: Partition, offset: Option[Offset])(f: ReadActions[Async] => Async[T]) = {
        // TODO consider separate from splitting
        val consumer = {
          val timestamp = Platform.currentTime
          val consumer = consumerOf(key.topic) // TODO ~10ms
          val duration = Platform.currentTime - timestamp
          // TODO add metric
          log.debug(s"$key consumerOf() took $duration ms")
          consumer
        }

        val topicPartition = TopicPartition(topic = key.topic, partition = partition)
        consumer.assign(Nel(topicPartition))

        offset match {
          case None =>
            log.warn(s"$key consuming from offset: 0")
            consumer.seekToBeginning(Nel(topicPartition))

          case Some(offset) =>
            val from = offset + 1
            log.debug(s"$key consuming from offset: $from")
            consumer.seek(topicPartition, from)
        }

        val readKafka = ReadActions(consumer, pollTimeout, log)
        val result = f(readKafka)
        result.onComplete { _ =>
          consumer.close(closeTimeout).failed.foreach { failure =>
            log.error(s"$key failed to close consumer $failure", failure)
          }
        }
        result
      }
    }
  }
}
