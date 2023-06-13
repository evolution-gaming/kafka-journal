package com.evolutiongaming.kafka.journal.replicator

import cats.Parallel
import cats.effect.{Concurrent, Resource, Timer}
import cats.syntax.all._
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper.{BracketThrowable, MeasureDuration}
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.smetrics
import com.evolution.scache
import com.evolution.scache.{CacheMetrics, ExpiringCache}

import scala.concurrent.duration.FiniteDuration

trait CacheOf[F[_]] {

  def apply[K, V](topic: Topic): Resource[F, Cache[F, K, V]]
}

object CacheOf {

  def empty[F[_] : BracketThrowable]: CacheOf[F] = {
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

  @deprecated("Use `of1` instead", "0.2.1")
  def apply[F[_] : Concurrent : Timer : Parallel : smetrics.MeasureDuration](
    expireAfter: FiniteDuration,
    cacheMetrics: Option[CacheMetrics.Name => CacheMetrics[F]]
  ): CacheOf[F] = {
    implicit val md: MeasureDuration[F] = smetrics.MeasureDuration[F].toCatsHelper
    apply1(expireAfter, cacheMetrics)
  }

  def apply1[F[_] : Concurrent : Timer : Parallel : MeasureDuration](
    expireAfter: FiniteDuration,
    cacheMetrics: Option[CacheMetrics.Name => CacheMetrics[F]]
  ): CacheOf[F] = {
    class Main
    new Main with CacheOf[F] {
      def apply[K, V](topic: Topic) = {
        val config = ExpiringCache.Config[F, K, V](expireAfter)
        for {
          cache <- scache.Cache.expiring(config)
          cache <- cacheMetrics.fold { cache.pure[Resource[F,*]] } { cacheMetrics => cache.withMetrics1(cacheMetrics(topic)) }
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
