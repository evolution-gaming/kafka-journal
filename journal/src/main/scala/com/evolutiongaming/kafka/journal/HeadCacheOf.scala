package com.evolutiongaming.kafka.journal

import cats.effect.Resource
import cats.effect.kernel.Async
import cats.effect.syntax.all.*
import cats.syntax.all.*
import cats.{Applicative, Parallel}
import com.evolutiongaming.catshelper.{FromTry, LogOf, MeasureDuration, Runtime}
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.skafka.consumer.ConsumerConfig

trait HeadCacheOf[F[_]] {

  def apply(
      consumerConfig: ConsumerConfig,
      eventualJournal: EventualJournal[F],
  ): Resource[F, HeadCache[F]]
}

object HeadCacheOf {

  def empty[F[_]: Applicative]: HeadCacheOf[F] = const(HeadCache.empty[F].pure[F].toResource)

  def const[F[_]](value: Resource[F, HeadCache[F]]): HeadCacheOf[F] = { (_: ConsumerConfig, _: EventualJournal[F]) =>
    value
  }

  def apply[F[_]](implicit F: HeadCacheOf[F]): HeadCacheOf[F] = F

  def apply[F[_]: Async: Parallel: Runtime: LogOf: KafkaConsumerOf: MeasureDuration: FromTry: FromJsResult: JsonCodec.Decode](
      metrics: Option[HeadCacheMetrics[F]],
  ): HeadCacheOf[F] = { (consumerConfig: ConsumerConfig, eventualJournal: EventualJournal[F]) =>
    {
      HeadCache.make[F](consumerConfig, eventualJournal, metrics)
    }
  }
}
