package com.evolutiongaming.kafka.journal

import cats.Monad
import cats.implicits._
import com.evolutiongaming.kafka.journal.conversions.ConsumerRecordToActionRecord
import com.evolutiongaming.skafka.consumer.ConsumerRecords
import scodec.bits.ByteVector

object ReadActions {

  // TODO list
  type Type[F[_]] = F[Iterable[ActionRecord[Action]]]

  def apply[F[_] : Monad](
    key: Key,
    consumer: Journal.Consumer[F])(implicit
    consumerRecordToActionRecord: ConsumerRecordToActionRecord[F]
  ): Type[F] = {

    def actionRecords(records: ConsumerRecords[String, ByteVector]) = {
      val records1 = for {
        records <- records.values.values.toList
        record  <- records.toList if record.key.exists(_.value == key.id)
      } yield record
      records1.traverseFilter(consumerRecordToActionRecord.apply)
    }

    for {
      records <- consumer.poll
      records <- actionRecords(records)
    } yield records
  }
}
