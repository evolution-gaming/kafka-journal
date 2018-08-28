package com.evolutiongaming.kafka.journal

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.skafka.{Offset, Partition}

trait FoldActions {
  def apply[S](offset: Option[Offset], s: S)(f: Fold[S, Action.User]): Async[S]
}

object FoldActions {

  val Empty: FoldActions = new FoldActions {
    def apply[S](offset: Option[Offset], s: S)(f: Fold[S, Action.User]) = s.async
  }

  // TODO add range argument
  def apply(
    key: Key,
    from: SeqNr,
    marker: Marker,
    offsetReplicated: Option[Offset],
    withReadActions: WithReadActions[Async]): FoldActions = {

    // TODO compare partitions !
    val partition = marker.partition

    val replicated = offsetReplicated.exists(_ >= marker.offset)

    if (replicated) Empty
    else new FoldActions {

      def apply[S](offset: Option[Offset], s: S)(f: Fold[S, Action.User]) = {

        val max = marker.offset - 1

        val replicated = offset.exists(_ >= max)

        if (replicated) s.async
        else {
          // TODO use max form Helpers
          val partitionOffset = {
            val max = PartialFunction.condOpt((offset, offsetReplicated)) {
              case (Some(x), Some(y)) => x max y
              case (Some(x), None)    => x
              case (None, Some(x))    => x
            }

            for {offset <- max} yield PartitionOffset(partition, offset)
          }

          withReadActions(key.topic, partitionOffset) { readActions =>

            val ff = (s: S) => {
              for {
                actions <- readActions(key.id)
              } yield {
                actions.foldWhile(s) { case (s, action) =>
                  val switch = action.action match {
                    case action: Action.Append => if (action.range.to < from) s.continue else f(s, action)
                    case action: Action.Delete => f(s, action)
                    case action: Action.Mark   => s switch action.header.id != marker.id
                  }
                  if (switch.stop) switch
                  else switch.switch(action.offset < max)
                }
              }
            }
            ff.foldWhile(s)
          }
        }
      }
    }
  }
}

final case class Marker(id: String, partitionOffset: PartitionOffset) {
  def offset: Offset = partitionOffset.offset
  def partition: Partition = partitionOffset.partition
}