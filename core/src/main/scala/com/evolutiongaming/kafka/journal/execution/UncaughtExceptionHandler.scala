package com.evolutiongaming.kafka.journal.execution

object UncaughtExceptionHandler {

  val default: Thread.UncaughtExceptionHandler = new Thread.UncaughtExceptionHandler {
    def uncaughtException(thread: Thread, error: Throwable) = {
      Thread.getDefaultUncaughtExceptionHandler match {
        case null    => error.printStackTrace()
        case handler => handler.uncaughtException(Thread.currentThread(), error)
      }
    }
  }
}