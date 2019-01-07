package com.evolutiongaming.kafka.journal

import cats.Applicative
import cats.implicits._
import com.evolutiongaming.skafka.{Offset, Partition}

import scala.collection.immutable.Queue

object WithPollActionsOneByOne {

  def apply[F[_] : Applicative](actions: => Queue[ActionRecord[Action]]): WithPollActions[F] = new WithPollActions[F] {

    def apply[T](key: Key, partition: Partition, offset: Option[Offset])(f: PollActions[F] => F[T]) = {

      val pollActions = new PollActions[F] {

        var left = offset match {
          case None         => actions
          case Some(offset) => actions.dropWhile(_.offset <= offset)
        }

        def apply() = {
          left.dequeueOption.fold(Iterable.empty[ActionRecord[Action]].pure[F]) { case (record, left) =>
            this.left = left
            List(record).toIterable.pure[F]
          }
        }
      }

      f(pollActions)
    }
  }
}