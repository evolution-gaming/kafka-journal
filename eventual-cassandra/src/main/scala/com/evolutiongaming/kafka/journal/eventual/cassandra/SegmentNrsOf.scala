package com.evolutiongaming.kafka.journal.eventual.cassandra


import cats.syntax.all._
import cats.{Applicative, ~>}
import com.evolutiongaming.kafka.journal.Key

/** Calculate [[SegementNrs]] using the passed journal key.
  *
  * It is expected that for the same key the same [[SegmentNrs]] will be
  * returned.
  *
  * I.e. returning a constant value regardless a parameter is a valid,
  * though inefficient implementation.
  *
  * @see [[SegmentOf]] for a more simple implementation of this factory without
  * support of backwards compatible change of the segmenting algorithm.
  */
trait SegmentNrsOf[F[_]] {

  def apply(key: Key): F[SegmentNrs]
}

object SegmentNrsOf {

  /** Always return one and the same [[SegmentNrs]] instance.
    *
    * It is meant to be used in tests only as it could be a very inefficient way
    * in real life, i.e. all keys will get to one and the same Cassandra
    * partition.
    */
  def const[F[_]: Applicative](segmentNrs: SegmentNrs): SegmentNrsOf[F] = (_: Key) => segmentNrs.pure[F]

  /** Calculate both [[SegmentNrs]] values by a key using a hashing alorithm */
  def apply[F[_]: Applicative](first: Segments, second: Segments): SegmentNrsOf[F] = {
    key =>
      val hashCode = key.id.toLowerCase.hashCode
      val segmentNrs = SegmentNrs(
        first = SegmentNr(hashCode, first),
        second = SegmentNr(hashCode, second))
      segmentNrs.pure[F]
  }

  /** Calculate [[SegmentNrs#first]] value only and ignore [[SegmentNrs#second]] */
  def apply[F[_]: Applicative](segmentOf: SegmentOf[F]): SegmentNrsOf[F] = {
    key => segmentOf(key).map { segmentNr => SegmentNrs(segmentNr) }
  }

  implicit class SegmentNrsOfOps[F[_]](val self: SegmentNrsOf[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): SegmentNrsOf[G] = key => f(self(key))
  }
}
