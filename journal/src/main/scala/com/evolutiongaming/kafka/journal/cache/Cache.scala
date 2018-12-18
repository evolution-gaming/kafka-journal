package com.evolutiongaming.kafka.journal.cache

import cats.effect.Concurrent
import cats.effect.concurrent.{Deferred, Ref}
import cats.implicits._

trait Cache[F[_], K, V] {

  def getOrUpdate(k: K, v: F[V]): F[V]

  def values: F[Map[K, Deferred[F, V]]]
}

object Cache {

  def of[F[_] : Concurrent, K, V]: F[Cache[F, K, V]] = {
    for {
      ref <- Ref.of[F, Map[K, Deferred[F, V]]](Map.empty)
    } yield {

      new Cache[F, K, V] {

        // TODO add support of cancellation
        def getOrUpdate(k: K, v: F[V]) = {
          for {
            map <- ref.get
            v <- map.get(k).fold {
              for {
                d <- Deferred[F, V]
                v <- ref.modify { map =>
                  map.get(k).fold {
                    val value = for {
                      v <- v
                      _ <- d.complete(v)
                    } yield v
                    (map.updated(k, d), value)
                  } { d =>
                    (map, d.get)
                  }
                }
                v <- v
              } yield {
                v
              }
            } { d =>
              d.get
            }
          } yield {
            v
          }
        }

        def values = ref.get
      }
    }
  }
}