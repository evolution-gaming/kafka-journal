package akka.persistence.kafka.journal

import akka.persistence.AtomicWrite
import cats.Applicative
import cats.syntax.all._

trait Batching[F[_]] {
  def apply(aws: List[AtomicWrite]): F[List[List[AtomicWrite]]]
}

object Batching {

  def apply[F[_]](implicit F: Batching[F]): Batching[F] = F

  def disabled[F[_]: Applicative]: Batching[F] = (aws: List[AtomicWrite]) => aws.map(List(_)).pure[F]

  def all[F[_]: Applicative]: Batching[F] = (aws: List[AtomicWrite]) => List(aws).pure[F]

  def byNumberOfEvents[F[_]: Applicative](maxEventsInBatch: Int): Batching[F] = { (aws: List[AtomicWrite]) =>
    {
      GroupByWeight[AtomicWrite](aws, maxEventsInBatch)(_.payload.size).pure[F]
    }
  }
}
