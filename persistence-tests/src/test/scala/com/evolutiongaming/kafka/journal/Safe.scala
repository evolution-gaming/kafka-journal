package com.evolutiongaming.kafka.journal

import scala.util.control.NonFatal

object Safe {
  def apply[A](fa: => A): Unit = {
    try fa catch {
      case NonFatal(failure) => failure.printStackTrace()
    }
  }
}
