package com.evolutiongaming.kafka.journal

import cats.Apply
import cats.effect.{Concurrent, Resource}
import cats.syntax.all._
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.skafka.{Offset, Partition}
import cats.effect.Ref


object HeadCacheFenced {

  def of[F[_] : Concurrent](headCache: Resource[F, HeadCache[F]]): Resource[F, HeadCache[F]] = {

    val fence = Resource.make {
      Ref[F].of(().pure[F])
    } { fence =>
      fence.set(HeadCacheReleasedError.raiseError[F, Unit])
    }

    val result = for {
      headCache <- headCache
      fence     <- fence
    } yield {
      apply(headCache, fence.get.flatten)
    }
    result.fenced
  }

  def apply[F[_] : Apply](headCache: HeadCache[F], fence: F[Unit]): HeadCache[F] = {
    (key: Key, partition: Partition, offset: Offset) => {
      fence *> headCache.get(key, partition, offset)
    }
  }
}
