package com.evolutiongaming.kafka.journal

import cats.{Applicative, Parallel}
import cats.implicits._
import cats.effect.{Concurrent, ContextShift, Resource, Timer}
import com.evolutiongaming.catshelper.{FromTry, LogOf}
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.skafka.consumer.ConsumerConfig
import com.evolutiongaming.smetrics.MeasureDuration

trait HeadCacheOf[F[_]] {

  def apply(
    consumerConfig: ConsumerConfig,
    eventualJournal: EventualJournal[F]
  ): Resource[F, HeadCache[F]]
}

object HeadCacheOf {

  def empty[F[_] : Applicative]: HeadCacheOf[F] = const(Resource.liftF(HeadCache.empty[F].pure[F]))


  def const[F[_]](value: Resource[F, HeadCache[F]]): HeadCacheOf[F] = {
    (_: ConsumerConfig, _: EventualJournal[F]) => value
  }
  

  def apply[F[_]](implicit F: HeadCacheOf[F]): HeadCacheOf[F] = F


  def apply[F[_] : Concurrent : Parallel : Timer : ContextShift : LogOf : KafkaConsumerOf : MeasureDuration : FromTry : FromAttempt : FromJsResult](
    metrics: Option[HeadCacheMetrics[F]]
  ): HeadCacheOf[F] = {
    (consumerConfig: ConsumerConfig, eventualJournal: EventualJournal[F]) => {
      HeadCache.of[F](consumerConfig, eventualJournal, metrics)
    }
  }
}
