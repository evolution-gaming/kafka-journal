package com.evolutiongaming.kafka.journal.util

import cats.Applicative
import cats.effect.{Async, Sync}
import cats.implicits._
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

trait FromFuture[F[_]] {

  def apply[A](future: => Future[A])(): F[A]

  def listenable[A](future: => ListenableFuture[A]): F[A]
}

object FromFuture {

  def apply[F[_]](implicit f: FromFuture[F]): FromFuture[F] = f
  

  def lift[F[_] : Applicative : Async](implicit ec: ExecutionContextExecutor): FromFuture[F] = {
    
    new FromFuture[F] {

      def apply[A](future: => Future[A])() = {
        for {
          future <- Sync[F].delay(future)
          result <- future.value.fold {
            Async[F].async[A] { callback =>
              future.onComplete { a =>
                callback(a.toEither)
              }
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
            Futures.addCallback(future, futureCallback, ec)
          }
        } yield result
      }
    }
  }
}
