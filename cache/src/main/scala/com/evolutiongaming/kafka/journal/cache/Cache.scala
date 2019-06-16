package com.evolutiongaming.kafka.journal.cache

import cats.{Applicative, Monad}
import cats.effect.{Concurrent, Sync}
import cats.effect.concurrent.{Deferred, Ref}
import cats.implicits._
import com.evolutiongaming.catshelper.EffectHelper._
import com.evolutiongaming.catshelper.Runtime

trait Cache[F[_], K, V] {

  def get(key: K): F[Option[V]]

  def getOrUpdate(key: K)(value: => F[V]): F[V]

  def values: F[Map[K, F[V]]]
}

object Cache {


  def empty[F[_] : Applicative, K, V]: Cache[F, K, V] = new Cache[F, K, V] {

    def get(key: K) = none[V].pure[F]

    def getOrUpdate(key: K)(value: => F[V]) = value

    val values = Map.empty[K, F[V]].pure[F]
  }


  def of[F[_] : Concurrent : Runtime, K, V]: F[Cache[F, K, V]] = {
    for {
      cpus       <- Runtime[F].availableCores
      partitions  = 2 + cpus
      cache      <- of[F, K, V](partitions)
    } yield cache
  }


  def of[F[_] : Concurrent, K, V](nrOfPartitions: Int): F[Cache[F, K, V]] = {
    val cache = of[F, K, V](Map.empty[K, F[V]])
    for {
      partitions <- Partitions.of[F, K, Cache[F, K, V]](nrOfPartitions, _ => cache, _.hashCode())
    } yield {
      apply(partitions)
    }
  }


  def of[F[_] : Concurrent, K, V](map: Map[K, F[V]]): F[Cache[F, K, V]] = {
    for {
      ref <- Ref.of[F, Map[K, F[V]]](map)
    } yield {
      apply(ref)
    }
  }


  private def apply[F[_] : Concurrent, K, V](ref: Ref[F, Map[K, F[V]]]): Cache[F, K, V] = {
    new Cache[F, K, V] {

      def get(key: K) = {
        for {
          map   <- ref.get
          value <- map.get(key).fold(none[V].pure[F]) { _.map(_.some) }
        } yield value
      }

      def getOrUpdate(key: K)(value: => F[V]) = {

        def update = {

          def update(deferred: Deferred[F, F[V]]) = {
            Sync[F].uncancelable {
              for {
                value <- value.redeem[F[V], Throwable](_.raiseError[F, V], _.pure[F])
                _ <- deferred.complete(value)
                value <- value.attempt
                value <- value match {
                  case Right(value) => value.pure[F]
                  case Left(value)  => ref.modify { map => (map - key, value.raiseError[F, V]) }.flatten
                }
              } yield value
            }
          }

          for {
            deferred <- Deferred[F, F[V]]
            value1 <- ref.modify { map =>
              map.get(key).fold {
                val value1 = update(deferred)
                val map1 = map.updated(key, deferred.get.flatten)
                (map1, value1)
              } { value =>
                (map, value)
              }
            }
            value1 <- value1
          } yield value1
        }

        for {
          map <- ref.get
          value <- map.getOrElse(key, update)
        } yield value
      }

      def values = ref.get
    }
  }


  private def apply[F[_] : Monad, K, V](partitions: Partitions[K, Cache[F, K, V]]): Cache[F, K, V] = {

    new Cache[F, K, V] {

      def get(key: K) = {
        val cache = partitions.get(key)
        cache.get(key)
      }

      def getOrUpdate(key: K)(value: => F[V]) = {
        val cache = partitions.get(key)
        cache.getOrUpdate(key)(value)
      }

      def values = {
        val zero = Map.empty[K, F[V]]
        partitions.values.foldLeftM(zero) { (a, b) =>
          for {
            b <- b.values
          } yield {
            a ++ b
          }
        }
      }
    }
  }
}