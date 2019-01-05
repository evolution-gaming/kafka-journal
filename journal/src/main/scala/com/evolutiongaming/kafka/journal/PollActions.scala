package com.evolutiongaming.kafka.journal

import cats.FlatMap
import cats.implicits._
import com.evolutiongaming.kafka.journal.KafkaConverters._

import scala.concurrent.duration.FiniteDuration

trait PollActions[F[_]] {
  def apply(): F[Iterable[ActionRecord[Action]]]
}

object PollActions {

  def apply[F[_] : FlatMap](
    key: Key,
    consumer: KafkaConsumer[F, Id, Bytes],
    timeout: FiniteDuration): PollActions[F] = {

    new PollActions[F] {

      def apply() = {
        for {
          records <- consumer.poll(timeout)
        } yield for {
          records <- records.values.values
          record  <- records
          if record.key.exists(_.value == key.id)
          action  <- record.toAction
        } yield {
          val partitionOffset = PartitionOffset(record)
          ActionRecord(action, partitionOffset)
        }
      }
    }
  }
}
