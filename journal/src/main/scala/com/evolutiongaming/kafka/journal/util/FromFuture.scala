package com.evolutiongaming.kafka.journal.util

import cats.Applicative
import cats.effect.{Async, IO, Sync}
import cats.implicits._
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

// TODO not pass as argument, use Async instead
trait FromFuture[F[_]] {

  def apply[A](future: => Future[A])(): F[A]

  def listenable[A](future: => ListenableFuture[A]): F[A]
}

object FromFuture {

  private val immediate: ExecutionContext = new ExecutionContext {

    def execute(r: Runnable) = r.run()

    def reportFailure(e: Throwable) = {
      Thread.getDefaultUncaughtExceptionHandler match {
        case null => e.printStackTrace()
        case h    => h.uncaughtException(Thread.currentThread(), e)
      }
    }
  }

  
  def apply[F[_]](implicit f: FromFuture[F]): FromFuture[F] = f
  

  def lift[F[_] : Applicative : Async]: FromFuture[F] = {
    
    new FromFuture[F] {

      def apply[A](future: => Future[A])() = {
        for {
          future <- Sync[F].delay(future)
          result <- future.value.fold {
            Async[F].async[A] { callback =>
              future.onComplete(a => callback(a.toEither))(immediate)
            }
          } {
            case Success(a) => a.pure[F]
            case Failure(a) => a.raiseError[F, A]
          }
        } yield result
      }

      def listenable[A](future: => ListenableFuture[A]) = {
        for {
          future <- Sync[F].delay(future)
          result <- Async[F].async[A] { callback =>
            val futureCallback = new FutureCallback[A] {
              def onSuccess(a: A) = callback(a.asRight)
              def onFailure(t: Throwable) = callback(t.asLeft)
            }
            Futures.addCallback(future, futureCallback)
          }
        } yield result
      }
    }
  }

  def io(implicit F: Async[IO]): FromFuture[IO] = lift[IO]

  implicit def ioFromFuture(implicit F: Async[IO]): FromFuture[IO] = io
}
