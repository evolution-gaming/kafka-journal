package com.evolution.kafka.journal

import cats.effect.Resource
import cats.effect.kernel.Async
import cats.effect.syntax.all.*
import cats.syntax.all.*
import cats.{Applicative, Parallel}
import com.evolution.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.catshelper.{FromTry, LogOf, MeasureDuration, Runtime}
import com.evolutiongaming.skafka.consumer.ConsumerConfig

trait HeadCacheOf[F[_]] {

  def apply(
    consumerConfig: ConsumerConfig,
    eventualJournal: EventualJournal[F],
    config: HeadCacheConfig,
  ): Resource[F, HeadCache[F]]
}

object HeadCacheOf {

  def empty[F[_]: Applicative]: HeadCacheOf[F] = const(HeadCache.empty[F].pure[F].toResource)

  def const[F[_]](value: Resource[F, HeadCache[F]]): HeadCacheOf[F] = new Const(value)

  def apply[F[_]](
    implicit
    F: HeadCacheOf[F],
  ): HeadCacheOf[F] = F

  def apply[
    F[_]: Async: Parallel: Runtime: LogOf: KafkaConsumerOf: MeasureDuration: FromTry: FromJsResult: JsonCodec.Decode,
  ](
    metrics: Option[HeadCacheMetrics[F]],
  ): HeadCacheOf[F] = new Main(metrics)

  private final class Const[F[_]](value: Resource[F, HeadCache[F]]) extends HeadCacheOf[F] {
    def apply(
      consumerConfig: ConsumerConfig,
      eventualJournal: EventualJournal[F],
      config: HeadCacheConfig,
    ): Resource[F, HeadCache[F]] = value
  }

  private final class Main[
    F[_]: Async: Parallel: Runtime: LogOf: KafkaConsumerOf: MeasureDuration: FromTry: FromJsResult: JsonCodec.Decode,
  ](
    metrics: Option[HeadCacheMetrics[F]],
  ) extends HeadCacheOf[F] {
    def apply(
      consumerConfig: ConsumerConfig,
      eventualJournal: EventualJournal[F],
      config: HeadCacheConfig,
    ): Resource[F, HeadCache[F]] = {
      HeadCache.make[F](consumerConfig, eventualJournal, metrics, config)
    }
  }
}
