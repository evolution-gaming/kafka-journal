package com.evolutiongaming.kafka.journal

import cats.implicits._
import cats.{FlatMap, ~>}
import com.evolutiongaming.kafka.journal.KafkaConverters._

import scala.concurrent.duration.FiniteDuration

trait PollActions[F[_]] {
  // TODO remove ()
  def apply(): F[Iterable[ActionRecord[Action]]]
}

object PollActions {

  def apply[F[_] : FlatMap](
    key: Key,
    consumer: Journal.Consumer[F],
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


  implicit class PollActionsOps[F[_]](val self: PollActions[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): PollActions[G] = new PollActions[G] {
      def apply() = f(self())
    }
  }
}
