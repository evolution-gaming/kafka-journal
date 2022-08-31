package com.evolutiongaming.kafka.journal.util

import cats.effect.Resource
import cats.effect.kernel.Sync
import com.evolutiongaming.scache.{Cache, Releasable}

object CacheHelper {

  implicit class CacheOpsCacheHelper[F[_], K, V](val self: Cache[F, K, V]) extends AnyVal {

    def getOrUpdateResource(key: K)(load: => Resource[F, V])(implicit F: Sync[F]): F[V] = {
      self.getOrUpdateReleasable(key) { Releasable.of(load) }
    }
  }
}
