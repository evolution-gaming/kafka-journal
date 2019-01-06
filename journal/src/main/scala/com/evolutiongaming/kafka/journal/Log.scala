package com.evolutiongaming.kafka.journal

import cats.implicits._
import cats.{Applicative, ~>}
import cats.effect.Sync
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.safeakka.actor.ActorLog
import org.slf4j.{Logger, LoggerFactory}

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


  def empty[F[_]](unit: F[Unit]): Log[F] = new Log[F] {

    def debug(msg: => String) = unit

    def info(msg: => String) = unit

    def warn(msg: => String) = unit

    def error(msg: => String) = unit

    def error(msg: => String, cause: Throwable) = unit
  }

  def empty[F[_] : Applicative]: Log[F] = empty(Applicative[F].unit)


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


  def of[F[_] : Sync](subject: Class[_]): F[Log[F]] = {
    val name = subject.getName.stripSuffix("$")
    for {
      logger <- Sync[F].delay { LoggerFactory.getLogger(name) }
    } yield {
      apply[F](logger)
    }
  }
  

  def async(log: ActorLog): Log[Async] = new Log[Async] {

    def debug(msg: => String) = Async {
      log.debug(msg)
    }

    def info(msg: => String) = Async {
      log.info(msg)
    }

    def warn(msg: => String) = Async {
      log.warn(msg)
    }

    def error(msg: => String) = Async {
      log.error(msg)
    }

    def error(msg: => String, cause: Throwable) = Async {
      log.error(msg, cause)
    }
  }


  def fromLog[F[_] : Sync](log: ActorLog): Log[F] = new Log[F] {

    def debug(msg: => String) = {
      Sync[F].delay {
        log.debug(msg)
      }
    }

    def info(msg: => String) = {
      Sync[F].delay {
        log.info(msg)
      }
    }

    def warn(msg: => String) = {
      Sync[F].delay {
        log.warn(msg)
      }
    }

    def error(msg: => String, cause: Throwable) = {
      Sync[F].delay {
        log.error(msg, cause)
      }
    }

    def error(msg: => String) = {
      Sync[F].delay {
        log.error(msg)
      }
    }
  }


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