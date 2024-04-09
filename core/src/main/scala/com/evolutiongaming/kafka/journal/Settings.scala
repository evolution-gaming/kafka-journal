package com.evolutiongaming.kafka.journal

import cats.arrow.FunctionK
import cats.syntax.all._
import cats.{FlatMap, ~>}
import com.evolutiongaming.catshelper.{Log, MeasureDuration}
import com.evolutiongaming.sstream.Stream


trait Settings[F[_]] {

  type K = Setting.Key

  type V = Setting.Value

  def get(key: K): F[Option[Setting]]

  def set(key: K, value: V): F[Option[Setting]]

  @deprecated(
    message =
      "the behavior of the method might not be deterministic, " +
      "because it uses Cassandra LWTs, while other methods do not, " +
      "use get + set instead for a rough approximation of functionality",
    since = "3.3.9"
  )
  def setIfEmpty(key: K, value: V): F[Option[Setting]]

  def remove(key: K): F[Option[Setting]]

  def all: Stream[F, Setting]
}

object Settings {

  def apply[F[_]](implicit F: Settings[F]): Settings[F] = F


  implicit class SettingsOps[F[_]](val self: Settings[F]) extends AnyVal {

    def withLog(log: Log[F])(implicit F: FlatMap[F], measureDuration: MeasureDuration[F]): Settings[F] = {

      val functionKId = FunctionK.id[F]

      new Settings[F] {

        def get(key: K): F[Option[Setting]] = {
          for {
            d <- MeasureDuration[F].start
            r <- self.get(key)
            d <- d
            _ <- log.debug(s"$key get in ${ d.toMillis }ms, result: $r")
          } yield r
        }

        def set(key: K, value: V) = {
          for {
            d <- MeasureDuration[F].start
            r <- self.set(key, value)
            d <- d
            _ <- log.debug(s"$key set in ${ d.toMillis }ms, value: $value, result: $r")
          } yield r
        }

        def setIfEmpty(key: K, value: V) = {
          for {
            d <- MeasureDuration[F].start
            r <- self.setIfEmpty(key, value)
            d <- d
            _ <- log.debug(s"$key setIfEmpty in ${ d.toMillis }ms, value: $value, result: $r")
          } yield r
        }

        def remove(key: K) = {
          for {
            d <- MeasureDuration[F].start
            r <- self.remove(key)
            d <- d
            _ <- log.debug(s"$key set in ${ d.toMillis }ms, result: $r")
          } yield r
        }

        def all = {
          val logging = new (F ~> F) {
            def apply[A](fa: F[A]) = {
              for {
                d <- MeasureDuration[F].start
                r <- fa
                d <- d
                _ <- log.debug(s"all in ${ d.toMillis }ms, result: $r")
              } yield r
            }
          }
          self.all.mapK(logging, functionKId)
        }
      }
    }

    def mapK[G[_]](fg: F ~> G, gf: G ~> F): Settings[G] = new Settings[G] {

      def get(key: K) = fg(self.get(key))

      def set(key: K, value: V) = fg(self.set(key, value))

      def setIfEmpty(key: K, value: V) = fg(self.setIfEmpty(key, value))

      def remove(key: K) = fg(self.remove(key))

      def all = self.all.mapK(fg, gf)
    }
  }
}