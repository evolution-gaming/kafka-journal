package com.evolutiongaming.kafka.journal

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal.FoldWhileHelper._

import scala.concurrent.{ExecutionContext, Future}

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
    // TODO fix this
    override def foldUnit[A](iter: Iterable[Async[A]]) = Async.foldUnit(iter)
    def foldWhile[S](s: S)(f: S => Async[FoldWhileHelper.Switch[S]]) = f.foldWhile(s)
    def flatMapFailure[A, B >: A](fa: Async[A], f: Throwable => Async[B]) = fa.flatMapFailure(f)
  }


  implicit def futureToAsync(implicit ec: ExecutionContext): AdaptFuture[Async] = new AdaptFuture[Async] {
    def apply[A](future: Future[A]) = Async(future)
  }
}