package com.evolutiongaming.kafka.journal

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.kafka.journal.eventual.PartitionOffset
import com.evolutiongaming.skafka.{Offset, Partition, Topic}

trait FoldActions {
  def apply[S](offset: Option[Offset], s: S)(f: Fold[S, Action.User]): Async[S]
}

object FoldActions {

  val Empty: FoldActions = new FoldActions {
    def apply[S](offset: Option[Offset], s: S)(f: Fold[S, Action.User]) = s.async
  }

  // TODO add range argument
  def apply(
    id: Id,
    topic: Topic,
    from: SeqNr,
    marker: Marker,
    offsetReplicated: Option[Offset],
    withReadActions: WithReadActions): FoldActions = {

    // TODO compare partitions !
    val replicated = for {
      offsetLast <- marker.offset
      offsetReplicated <- offsetReplicated
    } yield {
      offsetLast.prev <= offsetReplicated
    }

    if (replicated getOrElse false) Empty
    else new FoldActions {

      def apply[S](offset: Option[Offset], s: S)(f: Fold[S, Action.User]) = {

        val replicated = for {
          offsetLast <- marker.offset
          offset <- offset
        } yield {
          offsetLast.prev <= offset
        }

        if (replicated getOrElse false) s.async
        else {
          val partitionOffset = {
            val offsetMax = PartialFunction.condOpt((offset, offsetReplicated)) {
              case (Some(o1), Some(o2)) => o1 max o2
              case (Some(o), None)      => o
              case (None, Some(o))      => o
            }

            for {offset <- offsetMax} yield PartitionOffset(marker.partition, offset)
          }

          withReadActions(topic, partitionOffset) { readActions =>

            val ff = (s: S) => {
              for {
                actions <- readActions(id)
              } yield {
                // TODO verify we did not miss Mark and not cycled infinitely
                actions.foldWhile(s) { (s, action) =>
                  action match {
                    case action: Action.Append => if (action.range.to < from) s.continue else f(s, action)
                    case action: Action.Delete => f(s, action)
                    case action: Action.Mark   => s switch action.header.id != marker.id
                  }
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

case class Marker(id: String, partition: Partition, offset: Option[Offset])