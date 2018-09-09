package com.evolutiongaming.kafka.journal

import scala.util.control.NonFatal

object Safe {
  def apply[T](f: => T): Unit = {
    try f catch {
      case NonFatal(failure) => failure.printStackTrace()
    }
  }
}
