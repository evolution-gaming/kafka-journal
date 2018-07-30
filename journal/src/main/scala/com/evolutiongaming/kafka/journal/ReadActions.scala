package com.evolutiongaming.kafka.journal

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.Alias.Id
import com.evolutiongaming.kafka.journal.KafkaConverters._
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.consumer.{Consumer, ConsumerRecord}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

trait ReadActions {
  def apply(id: Id): Async[Iterable[ActionRecord]]
}

object ReadActions {

  def apply(
    consumer: Consumer[String, Bytes],
    timeout: FiniteDuration,
    log: ActorLog)(implicit ec: ExecutionContext /*TODO remove*/): ReadActions = {

    def logSkipped(record: ConsumerRecord[String, Bytes]) = {
      def key = record.key getOrElse "none"

      def partitionOffset = PartitionOffset(record)

      // TODO important performance indication
      log.debug(s"ignoring key: $key, offset: $partitionOffset")
    }

    new ReadActions {

      def apply(id: Id) = {

        def filter(record: ConsumerRecord[String, Bytes]) = {
          val result = record.key contains id
          if (!result) {
            logSkipped(record)
          }
          result
        }


        for {
          consumerRecords <- consumer.poll(timeout).async
        } yield {
          for {
            consumerRecords <- consumerRecords.values.values
            consumerRecord <- consumerRecords
            if filter(consumerRecord)
            action <- consumerRecord.toAction
          } yield {
            ActionRecord(action, consumerRecord.offset)
          }
        }
      }
    }
  }
}
