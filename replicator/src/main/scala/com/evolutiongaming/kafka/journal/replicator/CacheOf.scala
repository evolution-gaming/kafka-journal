package com.evolutiongaming.kafka.journal.replicator

import cats.Parallel
import cats.effect.{Concurrent, Resource}
import cats.syntax.all._
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper.{BracketThrowable, Runtime}
import com.evolutiongaming.scache
import com.evolutiongaming.scache.{CacheMetrics, ExpiringCache, Releasable}
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.smetrics.MeasureDuration

import scala.concurrent.duration.FiniteDuration
import cats.effect.Temporal

trait CacheOf[F[_]] {

  def apply[K, V](topic: Topic): Resource[F, Cache[F, K, V]]
}

object CacheOf {

  def empty[F[_] : BracketThrowable]: CacheOf[F] = new CacheOf[F] {

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


  def apply[F[_] : Concurrent : Temporal : Runtime : Parallel : MeasureDuration](
    expireAfter: FiniteDuration,
    cacheMetrics: Option[CacheMetrics.Name => CacheMetrics[F]]
  ): CacheOf[F] = {
    new CacheOf[F] {
      def apply[K, V](topic: Topic) = {
        val config = ExpiringCache.Config[F, K, V](expireAfter)
        for {
          cache <- scache.Cache.expiring(config)
          cache <- cacheMetrics.fold { cache.pure[Resource[F,*]] } { cacheMetrics => cache.withMetrics(cacheMetrics(topic)) }
        } yield {
          new Cache[F, K, V] {

            def getOrUpdate(key: K)(value: => Resource[F, V]) = {
              cache.getOrUpdateReleasable(key) { Releasable.of(value) }
            }

            def remove(key: K) = cache.remove(key).flatten.void
          }
        }
      }
    }
  }
}
