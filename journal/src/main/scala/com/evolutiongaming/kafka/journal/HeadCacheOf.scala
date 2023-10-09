package com.evolutiongaming.kafka.journal

import cats.{Applicative, Parallel}
import cats.effect.Resource
import cats.effect.kernel.Async
import cats.effect.syntax.all._
import cats.syntax.all._
import com.evolutiongaming.catshelper.{FromTry, LogOf, MeasureDuration, Runtime}
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.skafka.consumer.ConsumerConfig

trait HeadCacheOf[F[_]] {

  @deprecated("use `apply1`", "2023-10-09")
  def apply(
    consumerConfig: ConsumerConfig,
    eventualJournal: EventualJournal[F]
  ): Resource[F, HeadCache[F]]

  def apply1(
    consumerConfig: ConsumerConfig,
    eventualJournal: EventualJournal[F],
    headCacheConfig: HeadCacheConfig
  ): Resource[F, HeadCache[F]]
}

object HeadCacheOf {

  def empty[F[_] : Applicative]: HeadCacheOf[F] = const(HeadCache.empty[F].pure[F].toResource)


  def const[F[_]](value: Resource[F, HeadCache[F]]): HeadCacheOf[F] = new HeadCacheOf[F] {

    def apply(
      consumerConfig: ConsumerConfig,
      eventualJournal: EventualJournal[F]
    ): Resource[F, HeadCache[F]] = value

    def apply1(
      consumerConfig: ConsumerConfig,
      eventualJournal: EventualJournal[F],
      headCacheConfig: HeadCacheConfig
    ): Resource[F, HeadCache[F]] = value
  }


  def apply[F[_]](implicit F: HeadCacheOf[F]): HeadCacheOf[F] = F


  def apply[F[_]: Async: Parallel: Runtime: LogOf: KafkaConsumerOf: MeasureDuration: FromTry: FromJsResult: JsonCodec.Decode](
    metrics: Option[HeadCacheMetrics[F]]
  ): HeadCacheOf[F] = new HeadCacheOf[F] {

    def apply(
      consumerConfig: ConsumerConfig,
      eventualJournal: EventualJournal[F]
    ): Resource[F, HeadCache[F]] =
      HeadCache.of1[F](consumerConfig, eventualJournal, metrics, HeadCacheConfig.default)

    def apply1(
      consumerConfig: ConsumerConfig,
      eventualJournal: EventualJournal[F],
      headCacheConfig: HeadCacheConfig
    ): Resource[F, HeadCache[F]] =
      HeadCache.of1[F](consumerConfig, eventualJournal, metrics, headCacheConfig)
  }
}
