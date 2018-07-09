package com.evolutiongaming.cassandra

import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.google.common.util.concurrent.ListenableFuture

import scala.concurrent.{ExecutionException, Future, Promise}
import scala.util.{Failure, Try}

object CassandraHelpers {

  implicit class ListenableFutureOps[T](val self: ListenableFuture[T]) extends AnyVal {

    def await(): Try[T] = {
      val safe = Try(self.get())
      safe.recoverWith { case failure: ExecutionException => Failure(failure.getCause) }
    }

    def asScala(): Future[T] = {
      if (self.isDone) {
        Future.fromTry(await())
      } else {
        val promise = Promise[T]
        val runnable = new Runnable {
          def run() = promise.complete(await())
        }
        self.addListener(runnable, CurrentThreadExecutionContext)
        promise.future
      }
    }
  }
}
