package com.evolutiongaming.kafka.journal

import cats.implicits._
import cats.{Applicative, Monad}
import com.evolutiongaming.skafka.{Offset, Partition}
import com.evolutiongaming.kafka.journal.stream.Stream

trait FoldActions[F[_]] {
  def apply(offset: Option[Offset]): stream.Stream[F, Action.User]
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
          new stream.Stream[F, Action.User] {

            def foldWhileM[L, R](l: L)(f: (L, Action.User) => F[Either[L, R]]) = {
              val last = offset max offsetReplicated
              withPollActions(key, partition, last) { pollActions =>

                val actions = for {
                  actions <- Stream.repeat(pollActions())
                  action <- Stream[F].apply(actions.toList /*TODO*/)
                } yield action

                val stream = actions.mapCmd { action =>
                  import Stream.Cmd
                  
                  if (action.offset > max) Stream.Cmd.stop
                  else action.action match {
                    case a: Action.Append => if (a.range.to < from) Cmd.skip else Cmd.take(a)
                    case a: Action.Delete => Cmd.take(a)
                    case a: Action.Mark   => if (a.id == marker.id) Cmd.stop else Cmd.skip
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