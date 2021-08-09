package com.evolutiongaming.kafka.journal

import akka.event.LoggingAdapter
import cats.effect.Sync
import com.evolutiongaming.catshelper.Log
import org.slf4j.MDC

object LogFromAkka {

  def apply[F[_] : Sync](log: LoggingAdapter): Log[F] = new Log[F] {

    def debug(msg: => String, mdc: Log.Mdc) = {
      Sync[F].delay {
        if (log.isDebugEnabled) withMDC(mdc) { log.debug(msg) }
      }
    }

    def info(msg: => String, mdc: Log.Mdc) = {
      Sync[F].delay {
        if (log.isInfoEnabled) withMDC(mdc) { log.info(msg) }
      }
    }

    def warn(msg: => String, mdc: Log.Mdc) = {
      Sync[F].delay {
        if (log.isWarningEnabled) withMDC(mdc) { log.warning(msg) }
      }
    }

    def warn(msg: => String, cause: Throwable, mdc: Log.Mdc) = {
      Sync[F].delay {
        if (log.isWarningEnabled) withMDC(mdc) { log.warning(s"$msg: $cause") }
      }
    }

    def error(msg: => String, mdc: Log.Mdc) = {
      Sync[F].delay {
        if (log.isErrorEnabled) withMDC(mdc) { log.error(msg) }
      }
    }

    def error(msg: => String, cause: Throwable, mdc: Log.Mdc) = {
      Sync[F].delay {
        if (log.isErrorEnabled) withMDC(mdc) { log.error(cause, msg) }
      }
    }

    def withMDC(mdc: Log.Mdc)(log: => Unit): Unit =
      mdc.context match {
        case None => log
        case Some(mdc) =>
          val backup = MDC.getCopyOfContextMap
          MDC.clear()
          mdc.toSortedMap foreach { case (k, v) => MDC.put(k, v) }
          log
          MDC.setContextMap(backup)
    }
  }
}