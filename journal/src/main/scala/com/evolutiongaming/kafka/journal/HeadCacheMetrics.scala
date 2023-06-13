package com.evolutiongaming.kafka.journal

import cats.effect.Resource
import cats.{Applicative, Monad}
import com.evolutiongaming.smetrics.CollectorRegistry
import com.evolution.scache.CacheMetrics

import scala.concurrent.duration.FiniteDuration

final case class HeadCacheMetrics[F[_]](headCache: HeadCache.Metrics[F], cache: CacheMetrics[F])

object HeadCacheMetrics {

  def empty[F[_]: Applicative]: HeadCacheMetrics[F] = apply(HeadCache.Metrics.empty, CacheMetrics.empty)

  @deprecated("use another `apply` instead", "2023-02-13")
  def apply[F[_]](headCache: HeadCache.Metrics[F], cache: com.evolutiongaming.scache.CacheMetrics[F]): HeadCacheMetrics[F] = {
    throw new Exception("Deprecated, use another `apply` instead")
  }

  @deprecated("Use another `apply` instead", "2023-02-13")
  def apply[F[_]: Applicative](headCache: HeadCache.Metrics[F], cache: com.evolutiongaming.scache.CacheMetrics[F]): HeadCacheMetrics[F] = {
    apply(headCache, cache.toCacheMetrics)
  }

  def of[F[_]: Monad](
    registry: CollectorRegistry[F],
    prefix: HeadCache.Metrics.Prefix = HeadCache.Metrics.Prefix.default
  ): Resource[F, HeadCacheMetrics[F]] = {
    for {
      headCache <- HeadCache.Metrics.of(registry, prefix)
      cache     <- CacheMetrics.of(registry, s"${ prefix }_${ CacheMetrics.Prefix.Default }")
    } yield {
      apply(headCache, cache(prefix))
    }
  }


  private implicit class CacheMetrics0Ops[F[_]](val self: com.evolutiongaming.scache.CacheMetrics[F]) extends AnyVal {
    def toCacheMetrics(implicit F: Applicative[F]): CacheMetrics[F] = {
      new CacheMetrics[F] {
        def get(hit: Boolean) = self.get(hit)

        def load(time: FiniteDuration, success: Boolean) = self.load(time, success)

        def life(time: FiniteDuration) = self.life(time)

        def put = self.put

        def modify(entryExisted: Boolean, directive: CacheMetrics.Directive): F[Unit] = F.unit

        def size(size: Int) = self.size(size)

        def size(latency: FiniteDuration) = self.size(latency)

        def values(latency: FiniteDuration) = self.values(latency)

        def keys(latency: FiniteDuration) = self.keys(latency)

        def clear(latency: FiniteDuration) = self.clear(latency)

        def foldMap(latency: FiniteDuration) = self.foldMap(latency)
      }
    }
  }
}
