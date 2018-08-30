package com.evolutiongaming.kafka.journal

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.Alias.Id
import com.evolutiongaming.skafka.{Offset, TopicPartition}

import scala.collection.immutable.Queue

object WithReadActionsOneByOne {

  def apply(actions: => Queue[ActionRecord]): WithReadActions[Async] = new WithReadActions[Async] {

    def apply[T](topicPartition: TopicPartition, offset: Option[Offset])(f: ReadActions[Async] => Async[T]) = {

      val readActions = new ReadActions[Async] {

        var left = offset match {
          case None         => actions
          case Some(offset) => actions.dropWhile(_.offset <= offset)
        }

        def apply(id: Id) = {
          left.dequeueOption.fold(Async.nil[ActionRecord]) { case (record, left) =>
            this.left = left
            List(record).async
          }
        }
      }

      f(readActions)
    }
  }
}