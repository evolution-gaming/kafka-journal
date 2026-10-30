package com.evolution.kafka.journal.akka.persistence

import akka.persistence.AtomicWrite
import cats.Applicative
import cats.syntax.all.*

trait Batching[F[_]] {
  def apply(aws: List[AtomicWrite]): F[List[List[AtomicWrite]]]
}

object Batching {

  def apply[F[_]](
    implicit
    F: Batching[F],
  ): Batching[F] = F

  def disabled[F[_]: Applicative]: Batching[F] = new Batching[F] {
    def apply(aws: List[AtomicWrite]): F[List[List[AtomicWrite]]] = aws.map(List(_)).pure[F]
  }

  def all[F[_]: Applicative]: Batching[F] = new Batching[F] {
    def apply(aws: List[AtomicWrite]): F[List[List[AtomicWrite]]] = List(aws).pure[F]
  }

  def byNumberOfEvents[F[_]: Applicative](maxEventsInBatch: Int): Batching[F] = new Batching[F] {
    def apply(aws: List[AtomicWrite]): F[List[List[AtomicWrite]]] = {
      GroupByWeight[AtomicWrite](aws, maxEventsInBatch)(_.payload.size).pure[F]
    }
  }
}
