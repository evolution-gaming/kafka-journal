package com.evolutiongaming.kafka.journal

import cats.FlatMap
import cats.data.OptionT
import cats.implicits._
import com.evolutiongaming.skafka.consumer.ConsumerRecord
import scodec.bits.ByteVector

object ReadActions {

  type Type[F[_]] = F[Iterable[ActionRecord[Action]]]

  def apply[F[_] : FlatMap](
    key: Key,
    consumer: Journal.Consumer[F])(implicit
    consumerRecordToActionRecord: Conversion[OptionT[cats.Id, ?], ConsumerRecord[Id, ByteVector], ActionRecord[Action]]
  ): Type[F] = {

    for {
      records <- consumer.poll
    } yield for {
      records <- records.values.values
      record  <- records.toList if record.key.exists(_.value == key.id)
      action  <- consumerRecordToActionRecord(record).value
    } yield action
  }
}
