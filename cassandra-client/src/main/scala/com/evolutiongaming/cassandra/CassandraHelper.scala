package com.evolutiongaming.cassandra

import com.datastax.driver.core.{BoundStatement, Row}
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.google.common.util.concurrent.ListenableFuture

import scala.concurrent.{ExecutionException, Future, Promise}
import scala.util.{Failure, Try}

object CassandraHelper {

  implicit class ListenableFutureOps[T](val self: ListenableFuture[T]) extends AnyVal {

    // TODO
    def await(): Try[T] = {
      val safe = Try(self.get())
      safe.recoverWith { case failure: ExecutionException => Failure(failure.getCause) }
    }

    // TODO
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

  implicit class BoundStatementOps(val self: BoundStatement) extends AnyVal {
    def encode[T](name: String, value: T)(implicit encode: Encode[T]): BoundStatement = {
      encode(self, name, value)
    }
  }


  implicit class RowOps(val self: Row) extends AnyVal {
    def decode[T](name: String)(implicit decode: Decode[T]): T = {
      decode(self, name)
    }
  }
}
