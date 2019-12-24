package com.evolutiongaming.kafka.journal

import cats.data.{NonEmptyList => Nel}
import cats.effect.Resource
import cats.implicits._
import cats.~>
import com.evolutiongaming.catshelper.{BracketThrowable, Log}
import com.evolutiongaming.kafka.journal.conversions.ConsRecordToActionRecord
import com.evolutiongaming.skafka.{Offset, Partition, TopicPartition}
import com.evolutiongaming.sstream.Stream


trait ConsumeActionRecords[F[_]] {

  def apply(key: Key, partition: Partition, from: Offset): Stream[F, ActionRecord[Action]]
}

object ConsumeActionRecords {

  def apply[F[_] : BracketThrowable](
    consumer: Resource[F, Journal.Consumer[F]],
    log: Log[F])(implicit
    consRecordToActionRecord: ConsRecordToActionRecord[F]
  ): ConsumeActionRecords[F] = {
    (key: Key, partition: Partition, from: Offset) => {

      val topicPartition = TopicPartition(topic = key.topic, partition = partition)

      def seek(consumer: Journal.Consumer[F]) = {
        for {
          _ <- consumer.assign(Nel.of(topicPartition))
          _ <- consumer.seek(topicPartition, from)
          _ <- log.debug(s"$key consuming from $partition:$from")
        } yield {}
      }

      def filter(records: List[Nel[ConsRecord]]) = {
        for {
          records <- records
          record  <- records.toList if record.key.exists { _.value === key.id }
        } yield record
      }

      def poll(consumer: Journal.Consumer[F]) = {
        for {
          records0 <- consumer.poll
          records   = filter(records0.values.values.toList)
          actions  <- records.traverseFilter { a => consRecordToActionRecord(a).value }
        } yield actions
      }

      for {
        consumer <- Stream.fromResource(consumer)
        _        <- Stream.lift(seek(consumer))
        records  <- Stream.repeat(poll(consumer))
        record   <- Stream[F].apply(records)
      } yield record
    }
  }


  implicit class ConsumeActionRecordsOps[F[_]](val self: ConsumeActionRecords[F]) extends AnyVal {

    def mapK[G[_]](fg: F ~> G, gf: G ~> F): ConsumeActionRecords[G] = {
      (key: Key, partition: Partition, from1: Offset) => {
        self(key, partition, from1).mapK[G](fg, gf)
      }
    }
  }
}