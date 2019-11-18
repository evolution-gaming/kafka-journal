package com.evolutiongaming.kafka.journal.replicator

import cats.Parallel
import cats.effect.{Concurrent, Resource, Timer}
import cats.implicits._
import com.evolutiongaming.catshelper.{BracketThrowable, Runtime}
import com.evolutiongaming.scache
import com.evolutiongaming.scache.{CacheMetrics, Releasable}
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.smetrics.MeasureDuration

import scala.concurrent.duration.FiniteDuration

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

      Resource.liftF(cache.pure[F])
    }
  }


  def apply[F[_] : Concurrent : Timer : Runtime : Parallel : MeasureDuration](
    expireAfter: FiniteDuration,
    cacheMetrics: Option[CacheMetrics.Name => CacheMetrics[F]]
  ): CacheOf[F] = {
    new CacheOf[F] {
      def apply[K, V](topic: Topic) = {
        for {
          cache <- scache.Cache.expiring[F, K, V](expireAfter)
          cache <- cacheMetrics.fold { Resource.liftF(cache.pure[F]) } { cacheMetrics => cache.withMetrics(cacheMetrics(topic)) }
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
