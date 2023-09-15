package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.syntax.all._
import cats.{Applicative, ~>}
import com.evolutiongaming.kafka.journal.Key

/** Calculate [[SegementNr]] using the passed journal key.
  *
  * It is expected that for the same key the same [[SegmentNr]] will be
  * returned.
  *
  * I.e. returning a constant value such as [[SegmentNr#min]] regardless a
  * parameter is a valid, though inefficient implementation.
  *
  * @see [[SegmentNrsOf]] for an implementation supporting backwards compatible
  * change of the segmenting algorithm.
  */
trait SegmentOf[F[_]] {

  def apply(key: Key): F[SegmentNr]
}

object SegmentOf {

  /** Always return one and the same [[SegmentNr]] instance.
    *
    * It is meant to be used in tests only as it could be a very inefficient way
    * in real life, i.e. all keys will get to one and the same Cassandra
    * partition.
    */
  def const[F[_]: Applicative](segmentNr: SegmentNr): SegmentOf[F] = (_: Key) => segmentNr.pure[F]

  /** Calculate [[SegmentNr]] value by key using a hashing alorithm */
  def apply[F[_]: Applicative](segments: Segments): SegmentOf[F] = {
    key: Key => {
      val hashCode = key.id.toLowerCase.hashCode
      val segmentNr = SegmentNr(hashCode, segments)
      segmentNr.pure[F]
    }
  }


  implicit class SegmentOfOps[F[_]](val self: SegmentOf[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): SegmentOf[G] = (key: Key) => f(self(key))
  }
}
