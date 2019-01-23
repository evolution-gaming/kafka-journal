package com.evolutiongaming.kafka.journal

import cats.implicits._
import cats.{Applicative, FlatMap, Monad, ~>}
import com.evolutiongaming.kafka.journal.FoldWhile.Switch

trait Stream[F[_], A] { self =>

  def foldWhileM[L, R](l: L)(f: (L, A) => F[Either[L, R]]): F[Either[L, R]]


  final def foldWhile[L, R](l: L)(f: (L, A) => Either[L, R])(implicit F: Applicative[F]): F[Either[L, R]] = {
    foldWhileM[L, R](l) { (l, a) => f(l, a).pure[F] }
  }


  final def foldWhileSwitch[S](s: S)(f: (S, A) => Switch[S])(implicit flatMap: Monad[F]): F[Switch[S]] = {
    for {
      result <- foldWhile(s) { (s, a) =>
        val switch = f(s, a)
        if (switch.continue) switch.s.asLeft[S] else switch.s.asRight[S]
      }
    } yield {
      result.fold(l => Switch(l, continue = true), r => Switch(r, continue = false))
    }
  }


  final def fold[B](b: B)(f: (B, A) => B)(implicit F: Applicative[F]): F[B] = {
    for {
      result <- foldWhile(b) { (b, a) => f(b, a).asLeft[B] }
    } yield {
      result.merge
    }
  }


  final def toList(implicit F: Applicative[F]): F[List[A]] = {
    for {
      result <- fold(List.empty[A]) { (b, a) => a :: b }
    } yield {
      result.reverse
    }
  }


  final def first(implicit F: Applicative[F]): F[Option[A]] = {
    for {
      result <- foldWhile(none[A]) { (_, a) => a.some.asRight[Option[A]] }
    } yield {
      result.merge
    }
  }


  final def last(implicit F: Applicative[F]): F[Option[A]] = {
    for {
      result <- foldWhile(none[A]) { (_, a) => a.some.asLeft[Option[A]] }
    } yield {
      result.merge
    }
  }


  final def map[B](f: A => B): Stream[F, B] = new Stream[F, B] {

    def foldWhileM[L, R](l: L)(f1: (L, B) => F[Either[L, R]]) = {
      self.foldWhileM(l) { (l, a)  => f1(l, f(a)) }
    }
  }


  final def flatMap[B](f: A => Stream[F, B]): Stream[F, B] = new Stream[F, B] {

    def foldWhileM[L, R](l: L)(f1: (L, B) => F[Either[L, R]]) = {
      self.foldWhileM(l) { (l, a) => f(a).foldWhileM(l)(f1) }
    }
  }
}

object Stream {

  def apply[F[_]](implicit F: Monad[F]): ApplyBuilders[F] = new ApplyBuilders[F](F)

  def lift[F[_], A](fa: F[A])(implicit monad: FlatMap[F]): Stream[F, A] = new Stream[F, A] {
    def foldWhileM[L, R](l: L)(f: (L, A) => F[Either[L, R]]) = fa.flatMap(f(l, _))
  }

  def from[F[_], G[_], A](ga: G[A])(implicit G: FoldWhile1[G], monad: Monad[F]): Stream[F, A] = new Stream[F, A] {
    def foldWhileM[L, R](l: L)(f: (L, A) => F[Either[L, R]]) = G.foldWhileM(ga, l)(f)
  }

  def empty[F[_], A](implicit F: Applicative[F]): Stream[F, A] = new Stream[F, A] {
    def foldWhileM[L, R](l: L)(f: (L, A) => F[Either[L, R]]) = l.asLeft[R].pure[F]
  }


  final class ApplyBuilders[F[_]](val F: Monad[F]) extends AnyVal {

    def apply[G[_], A](ga: G[A])(implicit G: FoldWhile1[G]): Stream[F, A] = new Stream[F, A] {
      def foldWhileM[L, R](l: L)(f: (L, A) => F[Either[L, R]]) = G.foldWhileM(ga, l)(f)(F)
    }
  }


  implicit class StreamOps[F[_], A](val self: Stream[F, A]) extends AnyVal {

    def mapK[G[_]](to: F ~> G, from: G ~> F): Stream[G, A] = new Stream[G, A] {

      def foldWhileM[L, R](l: L)(f: (L, A) => G[Either[L, R]]) = {
        to(self.foldWhileM(l) { (l, a) => from(f(l, a)) })
      }
    }
  }
}
