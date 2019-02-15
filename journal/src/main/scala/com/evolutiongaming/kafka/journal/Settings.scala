package com.evolutiongaming.kafka.journal

import cats.arrow.FunctionK
import cats.effect.Clock
import cats.implicits._
import cats.{FlatMap, ~>}
import com.evolutiongaming.kafka.journal.stream.Stream

trait Settings[F[_]] {
  
  type K = Setting.Key
  
  type V = Setting.Value

  def get(key: K): F[Option[Setting]]

  def set(key: K, value: V): F[Option[Setting]]

  def setIfEmpty(key: K, value: V): F[Option[Setting]]

  def remove(key: K): F[Option[Setting]]

  def all: Stream[F, Setting]
}

object Settings {
  
  def apply[F[_]](implicit F: Settings[F]): Settings[F] = F


  implicit class SettingsOps[F[_]](val self: Settings[F]) extends AnyVal {

    def withLog(log: Log[F])(implicit F: FlatMap[F], clock: Clock[F]): Settings[F] = {

      val functionKId = FunctionK.id[F]

      new Settings[F] {

        def get(key: K): F[Option[Setting]] = {
          for {
            ab     <- Latency { self.get(key) }
            (r, l)  = ab
            _      <- log.debug(s"$key get in ${ l }ms, result: $r")
          } yield r
        }

        def set(key: K, value: V) = {
          for {
            ab     <- Latency { self.set(key, value) }
            (r, l)  = ab
            _      <- log.debug(s"$key set in ${ l }ms, value: $value, result: $r")
          } yield r
        }

        def setIfEmpty(key: K, value: V) = {
          for {
            ab     <- Latency { self.setIfEmpty(key, value) }
            (r, l)  = ab
            _      <- log.debug(s"$key setIfEmpty in ${ l }ms, value: $value, result: $r")
          } yield r
        }

        def remove(key: K) = {
          for {
            ab     <- Latency { self.remove(key) }
            (r, l)  = ab
            _      <- log.debug(s"$key set in ${ l }ms, result: $r")
          } yield r
        }

        def all = {
          val logging = new (F ~> F) {
            def apply[A](fa: F[A]) = {
              for {
                ab     <- Latency { fa }
                (r, l)  = ab
                _      <- log.debug(s"all in ${ l }ms, result: $r")
              } yield r
            }
          }
          self.all.mapK(logging, functionKId)
        }
      }
    }

    def mapK[G[_]](to: F ~> G, from: G ~> F): Settings[G] = new Settings[G] {

      def get(key: K) = to(self.get(key))

      def set(key: K, value: V) = to(self.set(key, value))

      def setIfEmpty(key: K, value: V) = to(self.setIfEmpty(key, value))

      def remove(key: K) = to(self.remove(key))

      def all = self.all.mapK(to, from)
    }
  }
}