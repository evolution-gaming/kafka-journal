package com.evolutiongaming.kafka.journal

import cats.Applicative
import cats.syntax.all._
import com.evolutiongaming.catshelper.BracketThrowable
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
import com.evolutiongaming.kafka.journal.util.StreamHelper._
import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.sstream.Stream

trait StreamActionRecords[F[_]] {

  /** Creates a stream of events from Kafka starting with specified offset.
    *
    * @param offset
    *   The offset to start the reading from. If `None` then reading will start
    *   from the beginning, which could either be the begining of Kafka topic
    *   partion, or another offset, if the stream implementation limits it to
    *   some newer offset.
    */
  def apply(offset: Option[Offset]): Stream[F, ActionRecord[Action.User]]
}

object StreamActionRecords {

  def empty[F[_] : Applicative]: StreamActionRecords[F] = {
    _: Option[Offset] => Stream.empty[F, ActionRecord[Action.User]]
  }

  /** Creates a reader for events not yet replicated to Cassandra.
    *
    * When [[StreamActionRecords!#apply]] is called, the reader will stream
    * messages starting with passed `offset` parameter, but not older than
    * `offsetReplicated` and `seqNr`, and not later than passed `marker`.
    *
    * I.e. the following order is expected:
    * {{{
    * marker.partition: ...offsetReplicated...from...offset...marker...
    * }}}
    *
    * If `offset` parameter passed to [[StreamActionRecords!#apply]] is outside
    * of these bounds, then empty stream will be returned indicating that there
    * is no unreplicated message with such offset.
    *
    * This is a main optimization comparing to what [[ConsumeActionRecords]]
    * does, in addition to filtering out [[Action.System]] records, so
    * previously sent markers etc. are not included.
    *
    * @param key
    *   Journal identifier.
    * @param from
    *   [[SeqNr]] of journal event to start reading from.
    * @param marker
    *   Marker to read the events until. No events will be read after market is
    *   encountered.
    * @param offsetReplicated
    *   Last known offset replicated to Cassandra, if any. No events will be
    *   read before this offset.
    * @param consumeActionRecords
    *   Underlying reader of Kafka records.
    */
  def apply[F[_] : BracketThrowable](
    // TODO add range argument
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
        max    <- marker.offset.dec[F].toStream
        result <- {
          val replicated = offset.exists { _ >= max }
          if (replicated) Stream.empty[F, ActionRecord[Action.User]]
          else {
            for {
              fromOffset <- offset
                .max(offsetReplicated)
                .fold(Offset.min.pure[F]) { _.inc[F] }
                .toStream
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
