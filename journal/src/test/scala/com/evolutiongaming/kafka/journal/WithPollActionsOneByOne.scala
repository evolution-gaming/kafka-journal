package com.evolutiongaming.kafka.journal

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.skafka.{Offset, Partition}

import scala.collection.immutable.Queue

object WithPollActionsOneByOne {

  def apply(actions: => Queue[ActionRecord[Action]]): WithPollActions[Async] = new WithPollActions[Async] {

    def apply[T](key: Key, partition: Partition, offset: Option[Offset])(f: PollActions[Async] => Async[T]) = {

      val pollActions = new PollActions[Async] {

        var left = offset match {
          case None         => actions
          case Some(offset) => actions.dropWhile(_.offset <= offset)
        }

        def apply() = {
          left.dequeueOption.fold(Async.nil[ActionRecord[Action]]) { case (record, left) =>
            this.left = left
            List(record).async
          }
        }
      }

      f(pollActions)
    }
  }
}