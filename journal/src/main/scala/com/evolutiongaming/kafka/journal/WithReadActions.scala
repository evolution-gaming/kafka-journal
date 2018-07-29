package com.evolutiongaming.kafka.journal

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.consumer.Consumer
import com.evolutiongaming.skafka.{Topic, TopicPartition}

import scala.compat.Platform
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration


// TODO pass partition even if offset is unknown
trait WithReadActions {
  def apply[T](topic: Topic, partitionOffset: Option[PartitionOffset])(f: ReadActions => Async[T]): Async[T]
}

object WithReadActions {

  def apply(
    newConsumer: Topic => Consumer[String, Bytes],
    pollTimeout: FiniteDuration,
    closeTimeout: FiniteDuration,
    log: ActorLog)(implicit ec: ExecutionContext /*TODO remove*/): WithReadActions = {

    new WithReadActions {

      def apply[T](topic: Topic, partitionOffset: Option[PartitionOffset])(f: ReadActions => Async[T]) = {

        // TODO consider separate from splitting
        val consumer = {
          val timestamp = Platform.currentTime
          val consumer = newConsumer(topic) // TODO ~10ms
          val duration = Platform.currentTime - timestamp
          log.debug(s"newConsumer() took $duration ms")
          consumer
        }

        partitionOffset match {
          case None =>
            val topics = List(topic)
            consumer.subscribe(topics, None) // TODO with listener

          case Some(partitionOffset) =>
            val topicPartition = TopicPartition(topic, partitionOffset.partition)
            consumer.assign(List(topicPartition))
            val offset = partitionOffset.offset + 1
            consumer.seek(topicPartition, offset)
        }


        val readKafka = ReadActions(consumer, pollTimeout)
        val result = f(readKafka)
        result.onComplete { _ =>
          consumer.close(closeTimeout).failed.foreach { failure =>
            log.error(s"failed to close consumer $failure", failure)
          }
        }
        result
      }
    }
  }
}
