package com.evolutiongaming.cassandra

import java.util.concurrent.Executor

import com.google.common.util.concurrent.ListenableFuture

import scala.concurrent.{Future, Promise}
import scala.util.Try

object Helpers {

  implicit class ListenableFutureOps[T](val self: ListenableFuture[T]) extends AnyVal {

    def asScala(executor: Executor): Future[T] = {
      val promise = Promise[T]
      val runnable = new Runnable {
        def run() = promise.complete(Try(self.get()))
      }
      self.addListener(runnable, executor)
      promise.future
    }
  }
}
