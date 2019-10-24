package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Applicative
import cats.implicits._
import com.evolutiongaming.kafka.journal.Key

trait SegmentNrOf[F[_]] {

  def apply(key: Key): F[SegmentNr]
}

object SegmentNrOf {

  def apply[F[_] : Applicative](segments: Segments): SegmentNrOf[F] = {
    key: Key => {
      val hashCode = key.id.hashCode
      val segmentNr = SegmentNr(hashCode, segments)
      segmentNr.pure[F]
    }
  }
}
