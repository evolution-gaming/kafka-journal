package com.evolutiongaming.kafka.journal

import com.evolutiongaming.concurrent.CurrentThreadExecutionContext

import scala.collection.immutable.Seq
import scala.concurrent.Future

// TODO move out
object FutureHelper {
  private val futureUnit = Future.successful(())
  private val futureNone = Future.successful(Option.empty)
  private val futureSeq = Future.successful(Seq.empty)


  implicit class FutureObjOps(val self: Future.type) extends AnyVal {
    def unit: Future[Unit] = futureUnit
    def none[T]: Future[Option[T]] = futureNone
    def seq[T]: Future[Seq[T]] = futureSeq
  }

  implicit class FutureOps[T](val self: Future[T]) extends AnyVal {
    def unit: Future[Unit] = self.map { _ => {} }(CurrentThreadExecutionContext)
  }
}
