package com.evolutiongaming.kafka.journal

import cats.Applicative
import cats.effect.Sync
import com.evolutiongaming.safeakka.actor.ActorLog
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


  def async[F[_] : IO2](log: ActorLog): Log[F] = new Log[F] {

    def debug(msg: => String) = IO2[F].effect {
      log.debug(msg)
    }

    def info(msg: => String) = IO2[F].effect {
      log.info(msg)
    }

    def warn(msg: => String) = IO2[F].effect {
      log.warn(msg)
    }

    def error(msg: => String) = IO2[F].effect {
      log.error(msg)
    }

    def error(msg: => String, cause: Throwable) = IO2[F].effect {
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
}