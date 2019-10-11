package com.evolutiongaming.kafka.journal

import cats.Applicative
import cats.implicits._
import com.evolutiongaming.catshelper.BracketThrowable
import com.evolutiongaming.skafka.{Offset, Partition}
import com.evolutiongaming.sstream.Stream

// TODO rename
trait FoldActions[F[_]] {
  def apply(offset: Option[Offset]): Stream[F, ActionRecord[Action.User]]
}

object FoldActions {

  def empty[F[_] : Applicative]: FoldActions[F] = new FoldActions[F] {
    def apply(offset: Option[Offset]) = Stream.empty[F, ActionRecord[Action.User]]
  }

  // TODO add range argument
  def apply[F[_] : BracketThrowable](
    key: Key,
    from: SeqNr,
    marker: Marker,
    offsetReplicated: Option[Offset],
    readActionsOf: ReadActionsOf[F]
  ): FoldActions[F] = {

    // TODO compare partitions !
    val partition = marker.partition

    val replicated = offsetReplicated.exists(_ >= marker.offset)

    if (replicated) empty[F]
    else new FoldActions[F] {

      def apply(offset: Option[Offset]) = {

        val max = marker.offset - 1

        val replicated = offset.exists(_ >= max)
        if (replicated) Stream.empty[F, ActionRecord[Action.User]]
        else {
          val last = offset max offsetReplicated
          val fromOffset = last.fold(Offset.Min)(_ + 1)
          val actions = for {
            readActions <- Stream[F].apply(readActionsOf(key, partition, fromOffset))
            actions     <- Stream[F].repeat(readActions)
            action      <- Stream[F].apply(actions.toList)
          } yield action

          actions.mapCmd { action =>
            import Stream.Cmd

            def take(a: Action.User) = Cmd.take(action.copy(action = a))

            if (action.offset > max) Stream.Cmd.stop
            else action.action match {
              case a: Action.Append => if (a.range.to < from) Cmd.skip else take(a)
              case a: Action.Delete => take(a)
              case a: Action.Mark   => if (a.id == marker.id) Cmd.stop else Cmd.skip
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