package org.apache.pekko.persistence.kafka.journal

import cats.Applicative
import cats.syntax.all.*
import org.apache.pekko.persistence.AtomicWrite

trait Batching[F[_]] {
  def apply(aws: List[AtomicWrite]): F[List[List[AtomicWrite]]]
}

object Batching {

  def apply[F[_]](
    implicit
    F: Batching[F],
  ): Batching[F] = F

  def disabled[F[_]: Applicative]: Batching[F] = (aws: List[AtomicWrite]) => aws.map(List(_)).pure[F]

  def all[F[_]: Applicative]: Batching[F] = (aws: List[AtomicWrite]) => List(aws).pure[F]

  def byNumberOfEvents[F[_]: Applicative](maxEventsInBatch: Int): Batching[F] = { (aws: List[AtomicWrite]) =>
    {
      GroupByWeight[AtomicWrite](aws, maxEventsInBatch)(_.payload.size).pure[F]
    }
  }
}
