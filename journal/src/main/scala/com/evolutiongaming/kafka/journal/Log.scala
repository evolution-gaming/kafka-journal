package com.evolutiongaming.kafka.journal

import akka.event.LoggingAdapter
import cats.effect.Sync
import cats.{Applicative, ~>}
import org.slf4j.Logger

trait Log[F[_]] { self =>

  def debug(msg: => String): F[Unit]

  def info(msg: => String): F[Unit]

  def warn(msg: => String): F[Unit]

  def error(msg: => String): F[Unit]

  def error(msg: => String, cause: Throwable): F[Unit]

  def prefixed(prefix: String): Log[F] = new Log[F] {

    def debug(msg: => String) = self.debug(s"$prefix $msg")

    def info(msg: => String) = self.info(s"$prefix $msg")

    def warn(msg: => String) = self.warn(s"$prefix $msg")

    def error(msg: => String) = self.error(s"$prefix $msg")

    def error(msg: => String, cause: Throwable) = self.error(s"$prefix $msg", cause)
  }
}

object Log {

  def apply[F[_]](implicit F: Log[F]): Log[F] = F


  def apply[F[_] : Sync](logger: Logger): Log[F] = new Log[F] {

    def debug(msg: => String) = {
      Sync[F].delay {
        if (logger.isDebugEnabled) logger.debug(msg)
      }
    }

    def info(msg: => String) = {
      Sync[F].delay {
        if (logger.isInfoEnabled) logger.info(msg)
      }
    }

    def warn(msg: => String) = {
      Sync[F].delay {
        if (logger.isWarnEnabled) logger.warn(msg)
      }
    }

    def error(msg: => String) = {
      Sync[F].delay {
        if (logger.isErrorEnabled) logger.error(msg)
      }
    }

    def error(msg: => String, cause: Throwable) = {
      Sync[F].delay {
        if (logger.isErrorEnabled) logger.error(msg, cause)
      }
    }
  }


  def apply[F[_] : Sync](log: LoggingAdapter): Log[F] = new Log[F] {

    def debug(msg: => String) = {
      Sync[F].delay {
        if (log.isDebugEnabled) log.debug(msg)
      }
    }

    def info(msg: => String) = {
      Sync[F].delay {
        if (log.isInfoEnabled) log.info(msg)
      }
    }

    def warn(msg: => String) = {
      Sync[F].delay {
        if (log.isWarningEnabled) log.warning(msg)
      }
    }

    def error(msg: => String) = {
      Sync[F].delay {
        if (log.isErrorEnabled) log.error(msg)
      }
    }

    def error(msg: => String, cause: Throwable) = {
      Sync[F].delay {
        if (log.isErrorEnabled) log.error(cause, msg)
      }
    }
  }


  def const[F[_]](unit: F[Unit]): Log[F] = new Log[F] {

    def debug(msg: => String) = unit

    def info(msg: => String) = unit

    def warn(msg: => String) = unit

    def error(msg: => String) = unit

    def error(msg: => String, cause: Throwable) = unit
  }

  def empty[F[_] : Applicative]: Log[F] = const(Applicative[F].unit)


  implicit class LogOps[F[_]](val self: Log[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): Log[G] = new Log[G] {

      def debug(msg: => String) = f(self.debug(msg))

      def info(msg: => String) = f(self.info(msg))

      def warn(msg: => String) = f(self.warn(msg))

      def error(msg: => String) = f(self.error(msg))

      def error(msg: => String, cause: Throwable) = f(self.error(msg, cause))
    }
  }
}