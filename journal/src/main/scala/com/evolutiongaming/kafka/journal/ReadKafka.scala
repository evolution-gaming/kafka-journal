package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.Alias.{Bytes, Id}
import com.evolutiongaming.kafka.journal.KafkaConverters._
import com.evolutiongaming.skafka.consumer.{Consumer, ConsumerRecord}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration

trait ReadKafka {
  def apply(id: Id): Future[Iterable[KafkaRecord.Any]]
}

object ReadKafka {

  def apply(
    consumer: Consumer[String, Bytes],
    timeout: FiniteDuration)(implicit ec: ExecutionContext/*TODO remove*/): ReadKafka = {

    def logSkipped(record: ConsumerRecord[String, Bytes]) = {
      val key = record.key getOrElse "none"
      val offset = record.offset
      val partition = record.partition
      // TODO important performance indication
      println(s"skipping unnecessary record key: $key, partition: $partition, offset: $offset")
    }

    new ReadKafka {
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
            record <- consumerRecord.toKafkaRecord
          } yield {
            record
          }
        }
      }
    }
  }
}
