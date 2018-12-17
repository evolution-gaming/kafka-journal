package com.evolutiongaming.kafka.journal

import cats.~>
import com.evolutiongaming.kafka.journal.FoldWhile._

import scala.annotation.tailrec

trait IO2[F[_]] {

  def pure[A](a: A): F[A]

  def point[A](a: => A): F[A]

  def fail[A](failure: Throwable): F[A]

  def effect[A](a: => A): F[A]

  def map[A, B](fa: F[A])(ab: A => B): F[B]

  def flatMap[A, B](fa: F[A])(afb: A => F[B]): F[B]

  def foldWhile[S](s: S)(f: S => F[S], b: S => Boolean): F[S]

  def flatMapFailure[A, B >: A](fa: F[A], f: Throwable => F[B]): F[B]

  def bracket[A, B](acquire: F[A])(release: A => F[Unit])(use: A => F[B]): F[B]

  def from[A, G[_]](g: G[A])(implicit arrow: G ~> F): F[A] = arrow(g)

  //  def to[A, G[_]](f: F[A])(implicit arrow: F ~> G): G[A] = arrow(f)

  final def foldUnit[A](iter: Iterable[F[A]]): F[Unit] = fold(iter, ()) { (_, a) => unit(a) }

  final def foldWhile1[S](s: S)(f: S => F[Switch[S]]): F[S] = {
    val result = foldWhile[Switch[S]](s.continue)(s => f(s.s), _.continue)
    map(result)(_.s)
  }

  final def fold[A, S](iter: Iterable[A], s: S)(f: (S, A) => F[S]): F[S] = {
    val iterator = iter.iterator
    foldWhile(s)(s => f(s, iterator.next()), _ => iterator.nonEmpty)
  }

  final def unit[A](fa: F[A]): F[Unit] = map(fa)(_ => ())

  final val unit: F[Unit] = pure(())


  private val ListF = pure(List.empty)

  final def nil[A]: F[List[A]] = cast(ListF)


  private val IterableF = pure(Iterable.empty)

  final def iterable[A]: F[Iterable[A]] = cast(IterableF)


  private val OptionF = pure(Option.empty)

  final def none[A]: F[Option[A]] = cast(OptionF)

  private def cast[A](fa: F[_]): F[A] = fa.asInstanceOf[F[A]]
}

object IO2 {

  def apply[F[_]](implicit F: IO2[F]): IO2[F] = F


  implicit val IdIO: IO2[cats.Id] = new IO2[cats.Id] {

    def pure[A](a: A) = a

    def point[A](a: => A) = a

    def effect[A](a: => A) = a

    def fail[A](failure: Throwable) = throw failure

    def map[A, B](fa: A)(ab: A => B) = ab(fa)

    def flatMap[A, B](fa: A)(afb: A => B) = afb(fa)

    def foldWhile[S](s: S)(f: S => S, b: S => Boolean) = {
      @tailrec def loop(s: S): S = if (b(s)) loop(f(s)) else s

      loop(f(s))
    }

    def flatMapFailure[A, B >: A](fa: A, f: Throwable => B) = fa

    def bracket[A, B](acquire: A)(release: A => Unit)(use: A => B) = {
      val a = acquire
      try use(a) finally release(a)
    }
  }


  object ops {

    implicit class IOOps[A, F[_]](val fa: F[A]) extends AnyVal {

      def map[B](f: A => B)(implicit F: IO2[F]): F[B] = F.map(fa)(f)

      def flatMap[B](afb: A => F[B])(implicit F: IO2[F]): F[B] = F.flatMap(fa)(afb)

      def flatMapFailure[B >: A](ftb: Throwable => F[B])(implicit F: IO2[F]): F[B] = F.flatMapFailure(fa, ftb)

      //      def flatMapTry[B](ab: Try[A] => F[B])(implicit F: IO[F]) = F.flatMapTry(fa)(ab)

      def bracket[B](release: A => F[Unit])(use: A => F[B])(implicit F: IO2[F]): F[B] = {
        F.bracket(fa)(release)(use)
      }

      def unit(implicit F: IO2[F]): F[Unit] = IO2[F].unit(fa)
    }

    implicit class IOIdOps[A](val self: A) extends AnyVal {
      def pure[F[_] : IO2]: F[A] = IO2[F].pure(self)
    }
  }
}