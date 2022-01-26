package com.evolutiongaming.kafka.journal

import cats.Applicative
import cats.effect.Resource
import cats.effect.kernel.Temporal
import cats.effect.syntax.all._
import cats.syntax.all._
import com.evolutiongaming.catshelper.{FromTry, LogOf, Runtime}
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

  def empty[F[_] : Applicative]: HeadCacheOf[F] = const(HeadCache.empty[F].pure[F].toResource)


  def const[F[_]](value: Resource[F, HeadCache[F]]): HeadCacheOf[F] = {
    (_: ConsumerConfig, _: EventualJournal[F]) => value
  }
  

  def apply[F[_]](implicit F: HeadCacheOf[F]): HeadCacheOf[F] = F


  def apply[F[_]: Temporal: Runtime: LogOf: KafkaConsumerOf: MeasureDuration: FromTry: FromJsResult: JsonCodec.Decode](
    metrics: Option[HeadCacheMetrics[F]]
  ): HeadCacheOf[F] = {
    (consumerConfig: ConsumerConfig, eventualJournal: EventualJournal[F]) => {
      for {
        headCache <- HeadCache.of[F](consumerConfig, eventualJournal, metrics)
        log       <- LogOf[F].apply(HeadCache.getClass).toResource
      } yield {
        headCache.withLog(log)
      }
    }
  }
}
