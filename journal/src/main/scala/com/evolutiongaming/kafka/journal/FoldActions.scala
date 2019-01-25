package com.evolutiongaming.kafka.journal

import cats.implicits._
import cats.{Applicative, Monad}
import com.evolutiongaming.kafka.journal.FoldWhile._
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
        else new Stream[F, Action.User] {

          def foldWhileM[L, R](l: L)(f: (L, Action.User) => F[Either[L, R]]) = {
            val last = offset max offsetReplicated
            withPollActions(key, partition, last) { pollActions =>

              // TODO can we rework this via stream operations ?

              l.tailRecM[F, Either[L, R]] { l =>
                for {
                  actions <- pollActions()
                  result <- actions.foldWhileM[F, L, Either[L, R]](l) { case (l, action) =>

                    def continue = l.asLeft[Either[L, R]].pure[F]

                    def stop = l.asLeft[R].asRight[L].pure[F]

                    def ff(a: Action.User) = {
                      for {
                        result <- f(l, a)
                      } yield for {
                        result <- result
                      } yield {
                        result.asRight[L]
                      }
                    }

                    if (action.offset > max) stop
                    else action.action match {
                      case a: Action.Append => if (a.range.to < from) continue else ff(a)
                      case a: Action.Delete => ff(a)
                      case a: Action.Mark   => if (a.id == marker.id) stop else continue
                    }
                  }
                } yield result
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