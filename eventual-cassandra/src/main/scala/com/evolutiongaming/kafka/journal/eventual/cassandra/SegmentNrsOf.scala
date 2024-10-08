package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.syntax.all.*
import cats.{Applicative, ~>}
import com.evolutiongaming.kafka.journal.Key

/** Calculate [[SegmentNrs]] from passed journal key for use in `metajournal` table.
  *
  * It is expected that for the same key the same [[SegmentNrs]] will be
  * returned.
  *
  * I.e. returning a constant value regardless a parameter is a valid, though
  * inefficient implementation.
  *
  * @see
  *   [[SegmentOf]] for a more simple implementation of this factory without
  *   support of backwards compatible change of the segmenting algorithm.
  */
@deprecated("use `SegmentNrs.Of` instead", "4.1.0")
private[journal] trait SegmentNrsOf[F[_]] {

  def apply(key: Key): F[SegmentNrs]
}

@deprecated("use `SegmentNrs.Of` instead", "4.1.0")
private[journal] object SegmentNrsOf {

  /** Always return one and the same [[SegmentNrs]] instance.
    *
    * It might be a very inefficient approach in some real life cases, i.e. all
    * keys will get to one and the same Cassandra partition.
    */
  def const[F[_]: Applicative](segmentNrs: SegmentNrs): SegmentNrsOf[F] = (_: Key) => segmentNrs.pure[F]

  /** Calculate both [[SegmentNrs]] values by a key using a hashing algorithm */
  def apply[F[_]: Applicative](first: Segments, second: Segments): SegmentNrsOf[F] = { key =>
    SegmentNrs(
      first  = SegmentNr.metaJournal(key, first),
      second = SegmentNr.metaJournal(key, second),
    ).pure[F]
  }

  /** Calculate [[SegmentNrs#first]] value only and ignore [[SegmentNrs#second]] */
  @deprecated("use `apply(first, second)` instead", "4.1.0")
  def apply[F[_]: Applicative](segmentOf: SegmentOf[F]): SegmentNrsOf[F] = { key =>
    segmentOf(key).map { segmentNr => SegmentNrs(segmentNr) }
  }

  implicit class SegmentNrsOfOps[F[_]](val self: SegmentNrsOf[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): SegmentNrsOf[G] = key => f(self(key))
  }
}
