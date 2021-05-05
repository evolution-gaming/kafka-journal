package com.evolutiongaming.kafka.journal.eventual.cassandra


import cats.syntax.all._
import cats.{Applicative, ~>}
import com.evolutiongaming.kafka.journal.Key

trait SegmentNrsOf[F[_]] {

  def apply(key: Key): F[SegmentNrs]
}

object SegmentNrsOf {

  def const[F[_]: Applicative](segmentNrs: SegmentNrs): SegmentNrsOf[F] = (_: Key) => segmentNrs.pure[F]

  def apply[F[_]: Applicative](first: Segments, second: Segments): SegmentNrsOf[F] = {
    key =>
      val hashCode = key.id.toLowerCase.hashCode
      val segmentNrs = SegmentNrs(
        first = SegmentNr(hashCode, first),
        second = SegmentNr(hashCode, second))
      segmentNrs.pure[F]
  }

  def apply[F[_]: Applicative](segmentOf: SegmentOf[F]): SegmentNrsOf[F] = {
    key => segmentOf(key).map { segmentNr => SegmentNrs(segmentNr) }
  }

  implicit class SegmentNrsOfOps[F[_]](val self: SegmentNrsOf[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): SegmentNrsOf[G] = key => f(self(key))
  }
}
