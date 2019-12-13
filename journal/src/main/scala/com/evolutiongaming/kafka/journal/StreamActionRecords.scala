package com.evolutiongaming.kafka.journal

import cats.Applicative
import cats.implicits._
import com.evolutiongaming.catshelper.BracketThrowable
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.sstream.Stream

trait StreamActionRecords[F[_]] {

  def apply(offset: Option[Offset]): Stream[F, ActionRecord[Action.User]]
}

object StreamActionRecords {

  def empty[F[_] : Applicative]: StreamActionRecords[F] = {
    _: Option[Offset] => Stream.empty[F, ActionRecord[Action.User]]
  }

  // TODO add range argument
  def apply[F[_] : BracketThrowable](
    key: Key,
    from: SeqNr,
    marker: Marker,
    offsetReplicated: Option[Offset],
    consumeActionRecords: ConsumeActionRecords[F]
  ): StreamActionRecords[F] = {

    // TODO compare partitions !
    val partition = marker.partition

    val replicated = offsetReplicated.exists { _ >= marker.offset }

    if (replicated) empty[F]
    else (offset: Option[Offset]) => {
      for {
        max    <- Stream.lift(marker.offset.dec[F])
        result <- {
          val replicated = offset.exists { _ >= max }
          if (replicated) Stream.empty[F, ActionRecord[Action.User]]
          else {
            val last = offset max offsetReplicated
            val fromOffset = last.fold(Offset.min.pure[F]) { _.inc[F] }
            for {
              fromOffset <- Stream.lift(fromOffset)
              result     <- consumeActionRecords(key, partition, fromOffset).stateless { record =>

                def take(action: Action.User) = {
                  (true, Stream[F].single(record.copy(action = action)))
                }

                def skip = {
                  (true, Stream[F].empty[ActionRecord[Action.User]])
                }

                def stop = {
                  (false, Stream[F].empty[ActionRecord[Action.User]])
                }

                if (record.offset > max) {
                  stop
                } else {
                  record.action match {
                    case a: Action.Append => if (a.range.to < from) skip else take(a)
                    case a: Action.Mark   => if (a.id === marker.id) stop else skip
                    case a: Action.Delete => take(a)
                    case a: Action.Purge  => take(a)
                  }
                }
              }

            } yield result
          }
        }
      } yield result
    }
  }
}