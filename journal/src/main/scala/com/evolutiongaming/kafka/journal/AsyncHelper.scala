package com.evolutiongaming.kafka.journal

import java.util.concurrent.TimeUnit

import cats.effect.{Clock, ExitCase, Sync}
import com.evolutiongaming.concurrent.async.Async

import scala.annotation.tailrec
import scala.collection.generic.CanBuildFrom
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


  implicit val SyncAsync: Sync[Async] = new Sync[Async] {

    def suspend[A](thunk: => Async[A]) = thunk

    def bracketCase[A, B](acquire: Async[A])(use: A => Async[B])(release: (A, ExitCase[Throwable]) => Async[Unit]) = {
      for {
        a <- acquire
        b <- use(a).flatMapFailure { error =>
          for {
            _ <- release(a, ExitCase.error(error)).recover { case _ => () }
            b <- Async.failed[B](error)
          } yield b
        }
      } yield b
    }

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