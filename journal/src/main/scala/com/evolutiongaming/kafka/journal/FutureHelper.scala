package com.evolutiongaming.kafka.journal

import com.evolutiongaming.concurrent.CurrentThreadExecutionContext

import scala.concurrent.Future

// TODO move out
object FutureHelper {
  private val futureUnit = Future.successful(())
  private val futureNone = Future.successful(None)


  implicit class FutureObjOps(val self: Future.type) extends AnyVal {
    def unit: Future[Unit] = futureUnit
    def none[T]: Future[Option[T]] = futureNone
  }

  implicit class FutureOps[T](val self: Future[T]) extends AnyVal {
    def unit: Future[Unit] = self.map { _ => {} }(CurrentThreadExecutionContext)
  }
}
