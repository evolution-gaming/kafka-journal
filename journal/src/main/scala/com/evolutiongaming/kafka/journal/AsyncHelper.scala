package com.evolutiongaming.kafka.journal

import java.util.concurrent.TimeUnit

import cats.MonadError
import cats.effect.Clock
import com.evolutiongaming.concurrent.async.Async

import scala.annotation.tailrec
import scala.collection.generic.CanBuildFrom
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

object AsyncHelper {

  implicit class AsyncOps(val self: Async.type) extends AnyVal {

    def seq[A, M[X] <: TraversableOnce[X]](as: M[Async[A]])(implicit cbf: CanBuildFrom[M[Async[A]], A, M[A]]): Async[M[A]] = {
      val b = Async(cbf(as))
      as.foldLeft(b) { (b, a) =>
        for {
          b <- b
          a <- a
        } yield {
          b += a
        }
      }.map(_.result())
    }

    def list[A](as: List[Async[A]]): Async[List[A]] = {
      val b = Async(List.empty[A])
      for {
        list <- as.foldLeft(b) { (b, a) =>
          for {
            b <- b
            a <- a
          } yield {
            a :: b
          }
        }
      } yield list.reverse
    }
  }


  implicit val IO2Async: IO2[Async] = new IO2[Async] {

    def pure[A](a: A) = Async(a)

    // TODO wrong implementation
    def point[A](a: => A) = Async(a)

    def effect[A](a: => A) = Async(a)

    def fail[A](failure: Throwable) = Async.failed(failure)

    def flatMap[A, B](fa: Async[A])(afb: A => Async[B]) = fa.flatMap(afb)

    def map[A, B](fa: Async[A])(ab: A => B) = fa.map(ab)

    def foldWhile[S](s: S)(f: S => Async[S], b: S => Boolean) = {

      import com.evolutiongaming.concurrent.async.Async._

      @tailrec def loop(s: S): Async[S] = {
        if (b(s)) {
          f(s) match {
            case Succeed(s)        => loop(s)
            case Failed(s)         => Failed(s)
            case s: InCompleted[S] => s.value() match {
              case Some(Success(s)) => loop(s)
              case Some(Failure(s)) => Failed(s)
              case None             => s.flatMap(break)
            }
          }
        } else {
          pure(s)
        }
      }

      def break(s: S) = loop(s)

      loop(s)
    }

    def flatMapFailure[A, B >: A](fa: Async[A], f: Throwable => Async[B]) = fa.flatMapFailure(f)

    def bracket[A, B](acquire: Async[A])(release: A => Async[Unit])(use: A => Async[B]) = {
      for {
        a <- acquire
        b = use(a)
        _ = b.onComplete { _ => release(a) }
        b <- b
      } yield b
    }
  }


  implicit def fromFuture(implicit ec: ExecutionContext): FromFuture2[Async] = new FromFuture2[Async] {

    def apply[A](fa: Future[A]) = {
      try Async(fa) catch {
        case NonFatal(failure) => Async.failed(failure)
      }
    }
  }

  implicit val MonadErrorAsync: MonadError[Async, Throwable] = new MonadError[Async, Throwable] {

    def raiseError[A](e: Throwable) = Async.failed(e)

    def handleErrorWith[A](fa: Async[A])(f: Throwable => Async[A]) = fa.flatMapFailure(f)

    def flatMap[A, B](fa: Async[A])(f: A => Async[B]) = fa.flatMap(f)

    def tailRecM[A, B](a: A)(f: A => Async[Either[A, B]]) = {

      @tailrec
      def apply(a: A): Async[B] = {
        import Async.{Failed, InCompleted, Succeed}

        f(a) match {
          case Succeed(Right(a))            => Succeed(a)
          case Succeed(Left(a))             => apply(a)
          case Failed(s)                    => Failed(s)
          case v: InCompleted[Either[A, B]] => v.value() match {
            case Some(Success(Right(a))) => Succeed(a)
            case Some(Success(Left(a)))  => apply(a)
            case Some(Failure(s))        => Failed(s)
            case None                    => v.flatMap {
              case Right(a) => Async(a)
              case Left(a)  => break(a)
            }
          }
        }
      }

      def break(a: A) = apply(a)

      apply(a)
    }

    def pure[A](x: A) = Async(x)
  }


  implicit val ClockAsync: Clock[Async] = new Clock[Async] {
    def realTime(unit: TimeUnit) = {
      Async(unit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS))
    }
    def monotonic(unit: TimeUnit) = {
      Async(unit.convert(System.nanoTime(), TimeUnit.NANOSECONDS))
    }
  }
}