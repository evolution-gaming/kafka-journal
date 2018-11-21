package com.evolutiongaming.kafka.journal


import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

object FutureImplicits {

  implicit def futureIO(implicit ec: ExecutionContext): IO[Future] = {

    def safe[A](f: => Future[A]): Future[A] = {
      try f catch {
        case NonFatal(failure) => Future.failed(failure)
      }
    }

    def cast[A](f: Future[_]): Future[A] = f.asInstanceOf[Future[A]]

    new IO[Future] {

      def pure[A](a: A) = Future.successful(a)

      // TODO wrong implementation
      def point[A](a: => A) = safe { Future.successful(a) }

      def effect[A](a: => A) = safe { Future.successful(a) }

      def flatMap[A, B](fa: Future[A])(afb: A => Future[B]) = {
        fa.value match {
          case Some(Success(a)) => safe { afb(a) }
          case Some(_)          => cast(fa)
          case None             => fa.flatMap(afb)
        }
      }

      def map[A, B](fa: Future[A])(ab: A => B) = {
        fa.value match {
          case Some(Success(a)) => safe { Future.successful(ab(a)) }
          case Some(_)          => cast(fa)
          case None             => fa.map(ab)
        }
      }

      def foldWhile[S](s: S)(f: S => Future[S], b: S => Boolean) = {

        @tailrec def loop(s: S): Future[S] = {
          if (b(s)) {
            val future = f(s)
            future.value match {
              case Some(Success(s)) => loop(s)
              case Some(Failure(_)) => future
              case None             => future.flatMap(break)
            }
          } else {
            pure(s)
          }
        }

        def break(s: S) = loop(s)

        loop(s)
      }

      def flatMapFailure[A, B >: A](fa: Future[A], f: Throwable => Future[B]) = {
        fa.value match {
          case Some(Success(_)) => fa
          case Some(Failure(a)) => safe { f(a) }
          case None             => fa.recoverWith[B] { case a => f(a) }
        }
      }
    }
  }


  implicit val fromFuture: FromFuture[Future] = new FromFuture[Future] {

    def apply[A](fa: => Future[A]) = {
      try fa catch {
        case NonFatal(failure) => Future.failed(failure)
      }
    }
  }
}
