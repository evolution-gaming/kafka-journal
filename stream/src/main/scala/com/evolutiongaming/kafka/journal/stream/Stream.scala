package com.evolutiongaming.kafka.journal.stream

import cats.effect.{Bracket, Resource}
import cats.implicits._
import cats.{Applicative, FlatMap, Monad, ~>}

trait Stream[F[_], A] { self =>

  import Stream.Cmd

  def foldWhileM[L, R](l: L)(f: (L, A) => F[Either[L, R]]): F[Either[L, R]]


  final def foldWhile[L, R](l: L)(f: (L, A) => Either[L, R])(implicit F: Applicative[F]): F[Either[L, R]] = {
    foldWhileM[L, R](l) { (l, a) => f(l, a).pure[F] }
  }


  final def fold[B](b: B)(f: (B, A) => B)(implicit F: Applicative[F]): F[B] = {
    for {
      result <- foldWhile(b) { (b, a) => f(b, a).asLeft[B] }
    } yield {
      result.merge
    }
  }


  final def foldM[B](b: B)(f: (B, A) => F[B])(implicit F: Applicative[F]): F[B] = {
    for {
      result <- foldWhileM(b) { (b, a) => f(b, a).map(_.asLeft[B]) }
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


  final def length(implicit F: Monad[F]): F[Long] = {
    fold(0l) { (n, _) =>  n + 1 }
  }


  final def take(n: Long)(implicit F: Monad[F]): Stream[F, A] = {
    foldMapCmd(n) { (n, a) => if (n > 0) (n - 1, Cmd.take(a)) else (n, Cmd.stop) }
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
      self.foldWhileM(l) { (l, a) => f1(l, f(a)) }
    }
  }


  final def mapM[B](f: A => F[B])(implicit F: FlatMap[F]): Stream[F, B] = new Stream[F, B] {

    def foldWhileM[L, R](l: L)(f1: (L, B) => F[Either[L, R]]) = {
      self.foldWhileM(l) { (l, a) => f(a).flatMap(b => f1(l, b)) }
    }
  }


  final def flatMap[B](f: A => Stream[F, B]): Stream[F, B] = new Stream[F, B] {

    def foldWhileM[L, R](l: L)(f1: (L, B) => F[Either[L, R]]) = {
      self.foldWhileM(l) { (l, a) => f(a).foldWhileM(l)(f1) }
    }
  }


  final def collect[B](pf: PartialFunction[A, B])(implicit F: Applicative[F]): Stream[F, B] = new Stream[F, B] {

    def foldWhileM[L, R](l: L)(f: (L, B) => F[Either[L, R]]) = {
      self.foldWhileM(l) { (l, a) => if (pf.isDefinedAt(a)) f(l, pf(a)) else l.asLeft[R].pure[F] }
    }
  }


  final def filter(f: A => Boolean)(implicit F: Applicative[F]): Stream[F, A] = new Stream[F, A] {

    def foldWhileM[L, R](l: L)(f1: (L, A) => F[Either[L, R]]) = {
      self.foldWhileM(l) { (l, a) => if (f(a)) f1(l, a) else l.asLeft[R].pure[F] }
    }
  }


  final def zipWithIndex(implicit F: Monad[F]): Stream[F, (A, Long)] = {
    foldMap(0l) { (l, a) => (l + 1, (a, l)) }
  }


  final def dropWhile(f: A => Boolean)(implicit F: Monad[F]): Stream[F, A] = {
    foldMapCmd(true) { (drop, a) => if (drop && f(a)) (drop, Cmd.skip) else (false, Cmd.take(a)) }
  }


  final def takeWhile(f: A => Boolean)(implicit F: Monad[F]): Stream[F, A] = {
    mapCmd { a => if (f(a)) Cmd.take(a) else Cmd.stop }
  }


  final def foldMapM[B, S](s: S)(f: (S, A) => F[(S, B)])(implicit F: Monad[F]): Stream[F, B] = new Stream[F, B] {

    def foldWhileM[L, R](l: L)(f1: (L, B) => F[Either[L, R]]) = {
      for {
        result <- self.foldWhileM((s, l)) { case ((s, l), a) =>
          for {
            ab     <- f(s, a)
            (s, b)  = ab
            result <- f1(l, b)
          } yield {
            result.leftMap { l => (s, l) }
          }
        }
      } yield {
        result.leftMap { case (_, l) => l }
      }
    }
  }

  
  final def foldMap[B, S](s: S)(f: (S, A) => (S, B))(implicit F: Monad[F]): Stream[F, B] = {
    foldMapM(s) { (s, a) => f(s, a).pure[F] }
  }


  final def foldMapCmdM[B, S](s: S)(f: (S, A) => F[(S, Cmd[B])])(implicit F: Monad[F]): Stream[F, B] = new Stream[F, B] {

    def foldWhileM[L, R](l: L)(f1: (L, B) => F[Either[L, R]]) = {
      for {
        result <- self.foldWhileM[(S, L), Either[L, R]]((s, l)) { case ((s, l), a) =>
          for {
            ab       <- f(s, a)
            (s, cmd)  = ab
            result   <- cmd match {
              case Cmd.Skip    => (s, l).asLeft[Either[L, R]].pure[F]
              case Cmd.Stop    => l.asLeft[R].asRight[(S, L)].pure[F]
              case Cmd.Take(b) => for {
                result <- f1(l, b)
              } yield result match {
                case Left(l) => (s, l).asLeft[Either[L, R]]
                case r       => r.asRight[(S, L)]
              }
            }
          } yield result
        }
      } yield result match {
        case Left((_, l)) => l.asLeft[R]
        case Right(r)     => r
      }
    }
  }


  final def foldMapCmd[B, S](s: S)(f: (S, A) => (S, Cmd[B]))(implicit F: Monad[F]): Stream[F, B] = {
    foldMapCmdM(s) { (s, a) => f(s, a).pure[F] }
  }


  final def mapCmdM[B](f: A => F[Cmd[B]])(implicit F: Monad[F]): Stream[F, B] = new Stream[F, B] {

    def foldWhileM[L, R](l: L)(f1: (L, B) => F[Either[L, R]]) = {
      for {
        result <- self.foldWhileM[L, Either[L, R]](l) { (l, a) =>
          for {
            cmd    <- f(a)
            result <- cmd match {
              case Cmd.Skip    => l.asLeft[Either[L, R]].pure[F]
              case Cmd.Stop    => l.asLeft[R].asRight[L].pure[F]
              case Cmd.Take(b) => for {
                result <- f1(l, b)
              } yield result match {
                case Left(l) => l.asLeft[Either[L, R]]
                case r       => r.asRight[L]
              }
            }
          } yield result
        }
      } yield {
        result.joinRight
      }
    }
  }


  final def mapCmd[B](f: A => Cmd[B])(implicit F: Monad[F]): Stream[F, B] = {
    mapCmdM { a => f(a).pure[F] }
  }


  final def drain(implicit F: Applicative[F]): F[Unit] = {
    val unit = ().asLeft[Unit].pure[F]
    for {
      result <- foldWhileM(()) { (_, _) => unit }
    } yield {
      result.merge
    }
  }
}

object Stream { self =>

  def apply[F[_]](implicit F: Monad[F]): Builders[F] = new Builders[F](F)

  def lift[F[_], A](fa: F[A])(implicit monad: FlatMap[F]): Stream[F, A] = new Stream[F, A] {
    def foldWhileM[L, R](l: L)(f: (L, A) => F[Either[L, R]]) = fa.flatMap(f(l, _))
  }

  def repeat[F[_], A](fa: F[A])(implicit F: Monad[F]): Stream[F, A] = new Stream[F, A] {

    def foldWhileM[L, R](l: L)(f: (L, A) => F[Either[L, R]]) = {
      for {
        r <- l.tailRecM { l =>
          for {
            a <- fa
            r <- f(l, a)
          } yield r
        }
      } yield {
        r.asRight
      }
    }
  }


  def from[F[_], G[_], A](ga: G[A])(implicit G: FoldWhile[G], monad: Monad[F]): Stream[F, A] = new Stream[F, A] {
    def foldWhileM[L, R](l: L)(f: (L, A) => F[Either[L, R]]) = G.foldWhileM(ga, l)(f)
  }


  def empty[F[_], A](implicit F: Applicative[F]): Stream[F, A] = new Stream[F, A] {
    def foldWhileM[L, R](l: L)(f: (L, A) => F[Either[L, R]]) = l.asLeft[R].pure[F]
  }


  def fromResource[F[_], A](resource: Resource[F, A])(implicit F: Bracket[F, Throwable]): Stream[F, A] = new Stream[F, A] {
    
    def foldWhileM[L, R](l: L)(f: (L, A) => F[Either[L, R]]) = {
      resource.use(a => f(l, a))
    }
  }


  final class Builders[F[_]](val F: Monad[F]) extends AnyVal {

    def apply[G[_], A](ga: G[A])(implicit G: FoldWhile[G]): Stream[F, A] = from[F, G, A](ga)(G, F)

    def apply[A](resource: Resource[F, A])(implicit F: Bracket[F, Throwable]): Stream[F, A] = {
      fromResource(resource)
    }

    def single[A](a: A): Stream[F, A] = new Stream[F, A] {
      def foldWhileM[L, R](l: L)(f: (L, A) => F[Either[L, R]]) = f(l, a)
    }

    def many[A](a: A, as: A*): Stream[F, A] = apply[List, A](a :: as.toList)

    def repeat[A](fa: F[A])(implicit F: Monad[F]): Stream[F, A] = self.repeat(fa)
  }


  sealed abstract class Cmd[+A] extends Product

  object Cmd {

    def take[A](value: A): Cmd[A] = Take(value)

    def stop[A]: Cmd[A] = Stop

    def skip[A]: Cmd[A] = Skip


    final case class Take[A] private(value: A) extends Cmd[A]

    final case object Skip extends Cmd[Nothing]

    final case object Stop extends Cmd[Nothing]
  }


  implicit class StreamOps[F[_], A](val self: Stream[F, A]) extends AnyVal {

    def mapK[G[_]](to: F ~> G, from: G ~> F): Stream[G, A] = new Stream[G, A] {

      def foldWhileM[L, R](l: L)(f: (L, A) => G[Either[L, R]]) = {
        to(self.foldWhileM(l) { (l, a) => from(f(l, a)) })
      }
    }
  }


  implicit class FlattenOps[F[_], A](val self: Stream[F, Stream[F, A]]) extends AnyVal {

    def flatten: Stream[F, A] = self.flatMap(identity)
  }
}
