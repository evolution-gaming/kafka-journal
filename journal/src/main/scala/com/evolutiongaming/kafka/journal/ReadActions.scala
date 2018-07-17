package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.Alias.Id
import com.evolutiongaming.kafka.journal.KafkaConverters._
import com.evolutiongaming.skafka.consumer.{Consumer, ConsumerRecord}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

trait ReadActions {
  def apply(id: Id): Future[Iterable[Action]]
}

object ReadActions {

  def apply(
    consumer: Consumer[String, Bytes],
    timeout: FiniteDuration)(implicit ec: ExecutionContext /*TODO remove*/): ReadActions = {

    def logSkipped(record: ConsumerRecord[String, Bytes]) = {
      val key = record.key getOrElse "none"
      val offset = record.offset
      val partition = record.partition
      // TODO important performance indication
      println(s"skipping unnecessary record key: $key, partition: $partition, offset: $offset")
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
          consumerRecords <- consumer.poll(timeout)
        } yield {
          for {
            consumerRecords <- consumerRecords.values.values
            consumerRecord <- consumerRecords
            if filter(consumerRecord) // TODO log skipped
            action <- consumerRecord.toAction
          } yield {
            action
          }
        }
      }
    }
  }
}
