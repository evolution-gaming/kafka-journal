package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.syntax.all._
import cats.{Applicative, ~>}
import com.evolutiongaming.kafka.journal.Key

trait SegmentOf[F[_]] {

  def apply(key: Key): F[SegmentNr]
}

object SegmentOf {

  def const[F[_] : Applicative](segmentNr: SegmentNr): SegmentOf[F] = (_: Key) => segmentNr.pure[F]

  def apply[F[_] : Applicative](segments: Segments): SegmentOf[F] = {
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
