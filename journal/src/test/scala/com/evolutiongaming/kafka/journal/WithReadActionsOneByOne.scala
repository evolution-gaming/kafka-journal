package com.evolutiongaming.kafka.journal

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.Alias.Id
import com.evolutiongaming.skafka.Topic

import scala.collection.immutable.Queue

object WithReadActionsOneByOne {

  def apply(actions: => Queue[ActionRecord]): WithReadActions = new WithReadActions {

    def apply[T](topic: Topic, partitionOffset: Option[PartitionOffset])(f: ReadActions => Async[T]) = {

      val readActions: ReadActions = new ReadActions {

        var left = partitionOffset.fold(actions) { partitionOffset =>
          actions.dropWhile(_.offset <= partitionOffset.offset)
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