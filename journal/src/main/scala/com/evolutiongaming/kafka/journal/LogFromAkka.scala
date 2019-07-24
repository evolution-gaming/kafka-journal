package com.evolutiongaming.kafka.journal

import akka.event.LoggingAdapter
import cats.effect.Sync
import com.evolutiongaming.catshelper.Log

object LogFromAkka {

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

    def warn(msg: => String, cause: Throwable) = {
      Sync[F].delay {
        if (log.isWarningEnabled) log.warning(s"$msg: $cause")
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
}