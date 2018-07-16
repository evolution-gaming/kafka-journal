package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.Alias.Bytes
import com.evolutiongaming.kafka.journal.eventual.PartitionOffset
import com.evolutiongaming.skafka.consumer.Consumer
import com.evolutiongaming.skafka.{Topic, TopicPartition}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}


// TODO pass partition even if offset is unknown
trait WithReadActions {
  def apply[T](topic: Topic, partitionOffset: Option[PartitionOffset])(f: ReadActions => Future[T]): Future[T]
}

object WithReadActions {

  def apply(
    newConsumer: () => Consumer[String, Bytes],
    pollTimeout: FiniteDuration,
    closeTimeout: FiniteDuration)(implicit ec: ExecutionContext /*TODO remove*/): WithReadActions = {

    new WithReadActions {

      def apply[T](topic: Topic, partitionOffset: Option[PartitionOffset])(f: ReadActions => Future[T]) = {

        // TODO blocking
        // TODO consider separate from splitting
        val consumer = newConsumer()

        partitionOffset match {
          case None =>
            val topics = List(topic)
            consumer.subscribe(topics) // TODO with listener
          //          consumer.seekToBeginning() // TODO

          case Some(partitionOffset) =>
            val topicPartition = TopicPartition(topic, partitionOffset.partition)
            consumer.assign(List(topicPartition)) // TODO blocking
          val offset = partitionOffset.offset + 1
            consumer.seek(topicPartition, offset) // TODO blocking
        }

        val readKafka = ReadActions(consumer, pollTimeout)
        val result = f(readKafka)
        result.onComplete { _ => consumer.close() } // TODO use timeout
        result
      }
    }
  }
}
