package com.evolutiongaming.kafka.journal

import cats.implicits._
import cats.{Applicative, Monad}
import com.evolutiongaming.skafka.{Offset, Partition}

trait FoldActions[F[_]] {
  def apply(offset: Option[Offset]): Stream[F, Action.User]
}

object FoldActions {

  def empty[F[_] : Applicative]: FoldActions[F] = new FoldActions[F] {
    def apply(offset: Option[Offset]) = Stream.empty[F, Action.User]
  }

  // TODO add range argument
  def apply[F[_] : Monad](
    key: Key,
    from: SeqNr,
    marker: Marker,
    offsetReplicated: Option[Offset],
    withPollActions: WithPollActions[F]): FoldActions[F] = {

    // TODO compare partitions !
    val partition = marker.partition

    val replicated = offsetReplicated.exists(_ >= marker.offset)

    if (replicated) empty[F]
    else new FoldActions[F] {

      def apply(offset: Option[Offset]) = {

        val max = marker.offset - 1

        val replicated = offset.exists(_ >= max)
        if (replicated) Stream.empty[F, Action.User]
        else {
          // TODO add support of Resources in Stream
          new Stream[F, Action.User] {

            def foldWhileM[L, R](l: L)(f: (L, Action.User) => F[Either[L, R]]) = {
              val last = offset max offsetReplicated
              withPollActions(key, partition, last) { pollActions =>

                val actions = for {
                  actions <- Stream.repeat(pollActions())
                  action <- Stream[F].apply(actions.toList /*TODO*/)
                } yield action

                val stream = actions.mapCmd { action =>
                  import Stream.Cmd
                  
                  if (action.offset > max) Stream.Cmd.Stop
                  else action.action match {
                    case a: Action.Append => if (a.range.to < from) Cmd.Skip else Cmd.Take(a)
                    case a: Action.Delete => Cmd.Take(a)
                    case a: Action.Mark   => if (a.id == marker.id) Cmd.Stop else Cmd.Skip
                  }
                }

                stream.foldWhileM(l)(f)
              }
            }
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