package com.evolutiongaming.async.concurrent

import com.evolutiongaming.concurrent.CurrentThreadExecutionContext

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

sealed trait Async[+T] {

  def map[TT](f: T => TT): Async[TT]

  def flatMap[TT](f: T => Async[TT]): Async[TT]

  def value: Option[Try[T]]

  def future: Future[T]

  def get(timeout: FiniteDuration): T

  def await(timeout: FiniteDuration): Async[T]

  def unit: Async[Unit] = map(_ => ())
}

object Async {

  def apply[T](value: T): Async[T] = Succeed(value)

  def apply[T](future: Future[T])(implicit ec: ExecutionContext): Async[T] = {
    future.value match {
      case Some(value) => completed(value)
      case None        => InCompleted(future)
    }
  }

  def succeed[T](value: T): Async[T] = Succeed(value)

  def failed[T](value: Throwable): Async[T] = Failed(value)

  def completed[T](value: Try[T]): Async[T] = Completed(value)

  def async[T](f: => T)(implicit ec: ExecutionContext): Async[T] = InCompleted(Future(f)(ec))

  def never[T]: Async[T] = Async(Future.never)(CurrentThreadExecutionContext)


  sealed trait Completed[+T] extends Async[T] {

    def await(timeout: FiniteDuration) = this
  }

  object Completed {
    def apply[T](value: Try[T]): Completed[T] = {
      value match {
        case Success(value) => Succeed(value)
        case Failure(value) => Failed(value)
      }
    }
  }


  final case class InCompleted[T] private(v: Future[T])(implicit ec: ExecutionContext) extends Async[T] {

    def map[TT](f: T => TT) = {
      v.value match {
        case Some(Success(v)) => safe { Succeed(f(v)) }
        case Some(Failure(v)) => Failed(v)
        case None             => InCompleted(v.map(f))
      }
    }

    def flatMap[TT](f: T => Async[TT]) = {
      v.value match {
        case Some(Success(v)) => safe { f(v) }
        case Some(Failure(v)) => Failed(v)
        case None             =>
          val result = for {
            v <- v
            v <- f(v).future
          } yield v
          InCompleted(result)
      }
    }

    def value = v.value

    def future = v

    def get(timeout: FiniteDuration) = Await.result(v, timeout)

    def await(timeout: FiniteDuration) = {
      v.value match {
        case Some(Success(v)) => Succeed(v)
        case Some(Failure(v)) => Failed(v)
        case None             => Completed(Try(Await.result(v, timeout)))
      }
    }

    override def toString = "Async(<not completed>)"
  }


  final case class Succeed[T] private(v: T) extends Completed[T] {

    def map[TT](f: T => TT) = safe { Succeed(f(v)) }

    def flatMap[TT](f: T => Async[TT]) = safe { f(v) }

    def value = Some(Success(v))

    def future = Future.successful(v)

    def get(timeout: FiniteDuration) = v

    override def toString = s"Async($v)"
  }


  final case class Failed private(v: Throwable) extends Completed[Nothing] {

    def map[TT](f: Nothing => TT) = this

    def flatMap[TT](f: Nothing => Async[TT]) = this

    def value = Some(Failure(v))

    def future = Future.failed(v)

    def get(timeout: FiniteDuration) = throw v

    override def toString = s"Async($v)"
  }


  implicit class FutureAsync[T](val self: Future[T]) extends AnyVal {
    def async(implicit ec: ExecutionContext): Async[T] = Async(self)
  }

  implicit class AnyAsync[T](val self: T) extends AnyVal {
    def async: Async[T] = Async(self)
  }


  private def safe[T](f: => Async[T]): Async[T] = try f catch { case NonFatal(failure) => Failed(failure) }
}
