package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.syntax.all.*
import cats.{Applicative, ~>}
import com.evolutiongaming.kafka.journal.Key

/**
 * Calculate [[SegmentNr]] from passed journal key for use in `metajournal` table.
 *
 * It is expected that for the same key the same [[SegmentNr]] will be returned.
 *
 * I.e. returning a constant value such as [[SegmentNr#min]] regardless a parameter is a valid,
 * though inefficient implementation.
 *
 * @see
 *   [[SegmentNrsOf]] for an implementation supporting backwards compatible change of the segmenting
 *   algorithm.
 * @see
 *   [[SegmentNr.metaJournal]] for the actual algorithm.
 */
@deprecated("use `SegmentNr.Of` instead", "4.1.0")
private[journal] trait SegmentOf[F[_]] {

  def apply(key: Key): F[SegmentNr]
}

@deprecated("use `SegmentNr.Of` instead", "4.1.0")
private[journal] object SegmentOf {

  /**
   * Always return one and the same [[SegmentNrs]] instance.
   *
   * It might be a very inefficient approach in some real life cases, i.e. all keys will get to one
   * and the same Cassandra partition.
   */
  def const[F[_]: Applicative](segmentNr: SegmentNr): SegmentOf[F] = (_: Key) => segmentNr.pure[F]

  /**
   * Calculate [[SegmentNr]] value by key using a hashing algorithm
   */
  def apply[F[_]: Applicative](segments: Segments): SegmentOf[F] = { (key: Key) =>
    SegmentNr.metaJournal(key, segments).pure[F]
  }

  implicit class SegmentOfOps[F[_]](val self: SegmentOf[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): SegmentOf[G] = (key: Key) => f(self(key))
  }
}
