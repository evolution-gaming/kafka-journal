package com.evolutiongaming.kafka.journal

import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.kafka.journal.Alias.SeqNr

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}

// TODO move out
object FutureHelper {
  private val futureUnit = ().future
  private val futureNone = Option.empty.future
  private val futureSeq = Seq.empty.future
  private val futureNil = Nil.future
  private val futureSeqNr = SeqNr.Min.future


  implicit class FutureObjOps(val self: Future.type) extends AnyVal {
    def unit: Future[Unit] = futureUnit
    def none[T]: Future[Option[T]] = futureNone
    def seq[T]: Future[Seq[T]] = futureSeq
    def nil[T]: Future[List[T]] = futureNil
    def seqNr[T]: Future[SeqNr] = futureSeqNr
  }

  implicit class FutureOps[T](val self: Future[T]) extends AnyVal {
    def unit: Future[Unit] = self.map { _ => {} }(CurrentThreadExecutionContext)
  }

  implicit class AnyOps[T](val self: T) extends AnyVal {
    def future: Future[T] = Future.successful(self)
  }
}
