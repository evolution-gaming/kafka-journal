package com.evolutiongaming.kafka.journal.cache

import cats.effect.Concurrent
import cats.effect.concurrent.{Deferred, Ref}
import cats.implicits._
import com.evolutiongaming.catshelper.EffectHelper._

trait Cache[F[_], K, V] {

  def get(key: K): F[Option[V]]

  def getOrUpdate(key: K)(value: => F[V]): F[V]

  def values: F[Map[K, F[V]]]
}

object Cache {

  def of[F[_] : Concurrent, K, V]: F[Cache[F, K, V]] = {
    for {
      ref <- Ref.of[F, Map[K, F[V]]](Map.empty)
    } yield {
      apply(ref)
    }
  }

  private def apply[F[_] : Concurrent, K, V](ref: Ref[F, Map[K, F[V]]]): Cache[F, K, V] = {
    new Cache[F, K, V] {

      def get(key: K) = {
        for {
          values <- values
          value  <- values.get(key).fold(none[V].pure[F]) { _.map(_.some) }
        } yield value
      }

      def getOrUpdate(key: K)(value: => F[V]) = {

        def update = {
          for {
            deferred <- Deferred[F, F[V]]
            value1   <- ref.modify { map =>
              map.get(key).fold {
                val value1 = Concurrent[F].uncancelable {
                  for {
                    value <- value.redeem[F[V], Throwable](_.raiseError[F, V], _.pure[F])
                    _     <- deferred.complete(value)
                    value <- value
                  } yield value
                }
                (map.updated(key, deferred.get.flatten), value1)
              } { value =>
                (map, value)
              }
            }
            value1 <- value1
          } yield value1
        }

        for {
          map   <- ref.get
          value <- map.getOrElse(key, update)
        } yield value
      }

      def values = ref.get
    }
  }
}