package com.evolutiongaming.kafka.journal.stream

import cats.{Foldable, Monad}
import cats.implicits._

// TODO
trait FoldWhile1[F[_]] {

  def foldWhileM[G[_], A, L, R](fa: F[A], l: L)(f: (L, A) => G[Either[L, R]])(implicit F: Monad[G]): G[Either[L, R]]

  def foldWhile[A, L, R](fa: F[A], l: L)(f: (L, A) => Either[L, R]): Either[L, R] = {
    foldWhileM[cats.Id, A, L, R](fa, l)(f)
  }
}

object FoldWhile1 {

  //  implicit val FoldWhileList: FoldWhile1[List] = foldWhileFoldable[List]

  //  implicit val FoldWhileIterable: FoldWhile1[Iterable] = ???

  implicit def foldWhileFoldable[F[_]](implicit foldable: Foldable[F]): FoldWhile1[F] = new FoldWhile1[F] {

    def foldWhileM[G[_], A, L, R](fa: F[A], l: L)(f: (L, A) => G[Either[L, R]])(implicit monad: Monad[G]) = {
      foldable.foldLeftM[G, A, Either[L, R]](fa, l.asLeft[R]) {
        case (Left(s), a) => f(s, a)
        case (b, _)       => b.pure[G]
      }
    }
  }


  implicit class FoldWhileId[F[_], A](val self: F[A]) extends AnyVal {

    def foldWhileM[G[_], L, R](l: L)(f: (L, A) => G[Either[L, R]])(implicit F: FoldWhile1[F], monad: Monad[G]): G[Either[L, R]] = {
      F.foldWhileM[G, A, L, R](self, l)(f)
    }

    def foldWhile[L, R](l: L)(f: (L, A) => Either[L, R])(implicit F: FoldWhile1[F]): Either[L, R] = {
      F.foldWhile[A, L, R](self, l)(f)
    }
  }
}