package com.evolutiongaming.kafka.journal

import cats.{Foldable, Monad}

import scala.util.control.ControlThrowable
import cats.implicits._


// TODO
trait FoldWhile1[F[_]] {
  
  def foldWhileM[G[_], A, L, R](fa: F[A], l: L)(f: (L, A) => G[Either[L, R]])(implicit F: Monad[G]): G[Either[L, R]]

  def foldWhile[A, L, R](fa: F[A], l: L)(f: (L, A) => Either[L, R]): Either[L, R] = {
    foldWhileM[cats.Id, A, L, R](fa, l)(f)
  }
}

object FoldWhile1 {

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

object FoldWhile {

  type Fold[S, E] = (S, E) => Switch[S]


  final case class Switch[@specialized +S](s: S, continue: Boolean) {
    def stop: Boolean = !continue
    def map[SS](f: S => SS): Switch[SS] = copy(s = f(s))
    def enclose: Switch[Switch[S]] = map(_ => this)

    def switch(continue: Boolean = true): Switch[S] = {
      if (continue == /*TODO use Eq*/ this.continue) this
      else copy(continue = continue)
    }
  }

  object Switch {
    val Unit: Switch[Unit] = continue(())

    def continue[@specialized S](s: S) = Switch(s, continue = true)
    def stop[@specialized S](s: S) = Switch(s, continue = false)
  }


  implicit class SwitchIdOps[S](val self: S) extends AnyVal {

    def continue: Switch[S] = Switch.continue(self)

    def stop: Switch[S] = Switch.stop(self)

    def switch(continue: Boolean): Switch[S] = Switch(self, continue)
  }


  implicit class IterableFoldWhile[A](val self: Iterable[A]) extends AnyVal {
    import IterableFoldWhile._

    def foldWhile[S](s: S)(f: Fold[S, A]): Switch[S] = {
      try {
        val ss = self.foldLeft(s) { (s, e) =>
          val switch = f(s, e)
          if (switch.stop) throw Return(switch) else switch.s
        }
        ss.continue
      } catch {
        case Return(switch) => switch.asInstanceOf[Switch[S]]
      }
    }

    def foldWhileM[F[_], B, S](s: S)(f: (S, A) => F[Either[S, B]])(implicit F: Monad[F]): F[Either[S, B]] = {
      FoldWhileM[F, List, A, B, S](self.toList)(s)(f)
    }
  }


  object FoldWhileM {

    def apply[F[_], G[_], A, B, S](
      ga: G[A])(
      s: S)(
      f: (S, A) => F[Either[S, B]])(implicit
      foldable: Foldable[G],
      monad: Monad[F]): F[Either[S, B]] = {

      foldable.foldLeftM[F, A, Either[S, B]](ga, s.asLeft[B]) {
        case (Left(s), a) => f(s, a)
        case (b, _)       => b.pure[F]
      }
    }
  }


  object IterableFoldWhile {
    private final case class Return[S](s: S) extends ControlThrowable
  }
}
