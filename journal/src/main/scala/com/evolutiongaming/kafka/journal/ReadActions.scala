package com.evolutiongaming.kafka.journal

import cats.Monad
import cats.data.OptionT
import cats.implicits._
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, ConsumerRecords}
import scodec.bits.ByteVector

object ReadActions {

  type Type[F[_]] = F[Iterable[ActionRecord[Action]]]

  def apply[F[_] : Monad](
    key: Key,
    consumer: Journal.Consumer[F])(implicit
    consumerRecordToActionRecord: Conversion[OptionT[F, ?], ConsumerRecord[Id, ByteVector], ActionRecord[Action]]
  ): Type[F] = {

    def actionRecords(records: ConsumerRecords[Id, ByteVector]) = {
      val records1 = for {
        records <- records.values.values.toList
        record  <- records.toList if record.key.exists(_.value == key.id)
      } yield record
      records1.traverseFilter { record => consumerRecordToActionRecord(record).value }
    }

    for {
      records <- consumer.poll
      records <- actionRecords(records)
    } yield records
  }
}
