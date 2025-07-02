package com.evolution.kafka.journal.replicator

import cats.Parallel
import cats.effect.Resource
import cats.effect.kernel.Temporal
import cats.effect.syntax.resource.*
import cats.syntax.all.*
import com.evolution.scache
import com.evolution.scache.{CacheMetrics, ExpiringCache}
import com.evolutiongaming.catshelper.{BracketThrowable, MeasureDuration, Runtime}
import com.evolutiongaming.skafka.Topic

import scala.concurrent.duration.FiniteDuration

private[journal] trait CacheOf[F[_]] {

  def apply[K, V](topic: Topic): Resource[F, Cache[F, K, V]]
}

private[journal] object CacheOf {

  def empty[F[_]: BracketThrowable]: CacheOf[F] = {
    class Empty
    new Empty with CacheOf[F] {

      def apply[K, V](topic: Topic) = {

        val cache = new Cache[F, K, V] {

          def getOrUpdate(key: K)(value: => Resource[F, V]) = value.use(_.pure[F])

          def remove(key: K) = ().pure[F]
        }

        cache
          .pure[F]
          .toResource
      }
    }
  }

  def apply[F[_]: Temporal: Runtime: Parallel: MeasureDuration](
    expireAfter: FiniteDuration,
    cacheMetrics: Option[CacheMetrics.Name => CacheMetrics[F]],
  ): CacheOf[F] = {
    class Main
    new Main with CacheOf[F] {
      def apply[K, V](topic: Topic) = {
        val config = ExpiringCache.Config[F, K, V](expireAfter)
        for {
          cache <- scache.Cache.expiring(config)
          cache <- cacheMetrics.fold { cache.pure[Resource[F, *]] } { cacheMetrics =>
            cache.withMetrics(cacheMetrics(topic))
          }
        } yield {
          new Cache[F, K, V] {

            def getOrUpdate(key: K)(value: => Resource[F, V]) = {
              cache.getOrUpdateResource(key) { value }
            }

            def remove(key: K) = cache.remove(key).flatten.void
          }
        }
      }
    }
  }
}
