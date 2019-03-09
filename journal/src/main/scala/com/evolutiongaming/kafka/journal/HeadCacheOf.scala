package com.evolutiongaming.kafka.journal

import cats.Applicative
import cats.effect.{Concurrent, ContextShift, Resource, Timer}
import cats.temp.par.Par
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.skafka.consumer.ConsumerConfig

trait HeadCacheOf[F[_]] {

  def apply(
    consumerConfig: ConsumerConfig,
    eventualJournal: EventualJournal[F]
  ): Resource[F, HeadCache[F]]
}

object HeadCacheOf {

  def apply[F[_]](implicit F: HeadCacheOf[F]): HeadCacheOf[F] = F


  def apply[F[_] : Concurrent : Par : Timer : ContextShift : LogOf : KafkaConsumerOf](
    metrics: Option[HeadCache.Metrics[F]]
  ) = new HeadCacheOf[F] {

    def apply(consumerConfig: ConsumerConfig, eventualJournal: EventualJournal[F]) = {
      HeadCache.of[F](consumerConfig, eventualJournal, metrics)
    }
  }

  def empty[F[_] : Applicative]: HeadCacheOf[F] = new HeadCacheOf[F] {

    def apply(consumerConfig: ConsumerConfig, eventualJournal: EventualJournal[F]) = {
      Resource.pure[F, HeadCache[F]](HeadCache.empty[F])
    }
  }
}
