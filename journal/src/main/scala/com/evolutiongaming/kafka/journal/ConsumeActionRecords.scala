package com.evolutiongaming.kafka.journal

import cats.data.{NonEmptyList => Nel}
import cats.effect.{Resource, Sync}
import cats.implicits._
import cats.{Monad, ~>}
import com.evolutiongaming.catshelper.{BracketThrowable, Log}
import com.evolutiongaming.kafka.journal.conversions.ConsumerRecordToActionRecord
import com.evolutiongaming.skafka.consumer.ConsumerRecords
import com.evolutiongaming.skafka.{Offset, Partition, TopicPartition}
import scodec.bits.ByteVector


trait ConsumeActionRecords[F[_]] {

  def apply(key: Key, partition: Partition, from: Offset): Resource[F, F[List[ActionRecord[Action]]]]
}

object ConsumeActionRecords {

  def apply[F[_] : Monad](
    consumer: Resource[F, Journal.Consumer[F]],
    log: Log[F])(implicit
    consumerRecordToActionRecord: ConsumerRecordToActionRecord[F]
  ): ConsumeActionRecords[F] = {
    (key: Key, partition: Partition, from: Offset) => {

      val topicPartition = TopicPartition(topic = key.topic, partition = partition)

      def actionRecords(consumer: Journal.Consumer[F]) = {
        for {
          _ <- consumer.assign(Nel.of(topicPartition))
          _ <- consumer.seek(topicPartition, from)
          _ <- log.debug(s"$key consuming from $partition:$from")
        } yield {
          def actionRecords(consumerRecords: ConsumerRecords[String, ByteVector]) = {
            val records = for {
              records <- consumerRecords.values.values.toList
              record  <- records.toList if record.key.exists { _.value == key.id }
            } yield record
            records.traverseFilter(consumerRecordToActionRecord.apply)
          }

          for {
            records <- consumer.poll
            records <- actionRecords(records)
          } yield records
        }
      }

      for {
        consumer      <- consumer
        actionRecords <- Resource.liftF(actionRecords(consumer))
      } yield actionRecords
    }
  }


  implicit class ConsumeActionRecordsOps[F[_]](val self: ConsumeActionRecords[F]) extends AnyVal {

    def mapK[G[_] : Sync](f: F ~> G)(implicit F: BracketThrowable[F]): ConsumeActionRecords[G] = {
      (key: Key, partition: Partition, from: Offset) => {
        for {
          a <- self(key, partition, from).mapK[G](f)
        } yield {
          f(a)
        }
      }
    }
  }
}