package com.evolutiongaming.kafka.journal

import com.evolutiongaming.concurrent.async.Async

import scala.concurrent.{ExecutionContext, Future}
import com.evolutiongaming.kafka.journal.FoldWhileHelper._

object AsyncHelper {

  implicit object AsyncIO extends IO[Async] {
    def pure[A](a: A) = Async(a)
    // TODO wrong implementation
    def point[A](a: => A) = Async(a)
    def flatMap[A, B](fa: Async[A], afb: A => Async[B]) = fa.flatMap(afb)
    def map[A, B](fa: Async[A], ab: A => B) = fa.map(ab)
    def unit[A] = Async.unit
    def unit[A](fa: Async[A]) = Async.unit
    def fold[A, S](iter: Iterable[A], s: S)(f: (S, A) => Async[S]) = Async.fold(iter, s)(f)
    def foldWhile[S](s: S)(f: S => Async[FoldWhileHelper.Switch[S]]) = f.foldWhile(s)
    def catchAll[A, B >: A](fa: Async[A], f: Throwable => Async[B]) = fa.catchAll(f)
  }


  implicit def futureToAsync(implicit ec: ExecutionContext): AdaptFuture[Async] = new AdaptFuture[Async] {
    def apply[A](future: Future[A]) = Async(future)
  }

  implicit class AsyncIdOps[A](val self: Async[A]) extends AnyVal {

    def catchAll[B >: A](f: Throwable => Async[B]): Async[B] = {
      self.redeem(f, Async(_))
    }
  }
}