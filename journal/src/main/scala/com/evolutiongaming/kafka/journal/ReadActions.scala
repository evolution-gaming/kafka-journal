package com.evolutiongaming.kafka.journal

import cats.FlatMap
import cats.implicits._
import com.evolutiongaming.kafka.journal.KafkaConverters._

object ReadActions {

  type Type[F[_]] = F[Iterable[ActionRecord[Action]]]

  def apply[F[_] : FlatMap](
    key: Key,
    consumer: Journal.Consumer[F]
  ): Type[F] = {

    for {
      records <- consumer.poll
    } yield for {
      records <- records.values.values
      record  <- records.toList if record.key.exists(_.value == key.id)
      action  <- record.toActionRecord
    } yield action
  }
}
