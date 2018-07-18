package com.evolutiongaming.concurrent.async

import com.evolutiongaming.concurrent.CurrentThreadExecutionContext

import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

sealed trait Async[+T] {

  def foreach[TT](f: T => TT): Unit

  def map[TT](f: T => TT): Async[TT]

  def flatMap[TT](f: T => Async[TT]): Async[TT]

  def mapTry[TT](f: Try[T] => Try[TT]): Async[TT]

  def value(): Option[Try[T]]

  def future: Future[T]

  def get(timeout: Duration = Duration.Inf): T

  def await(timeout: Duration = Duration.Inf): Async[T]

  def onComplete[TT](f: Try[T] => TT): Unit

  def recover[TT >: T](pf: PartialFunction[Throwable, TT]): Async[TT]

  def flatten[TT](implicit ev: T <:< Async[TT]): Async[TT] = flatMap(ev)

  final def unit: Async[Unit] = flatMap(_ => Async.unit)

  final def withFilter(p: T => Boolean): Async[T] = {
    map { v =>
      if (p(v)) v
      else throw new NoSuchElementException("Async.filter predicate is not satisfied")
    }
  }
}

object Async {
  import AsyncConverters._

  private val futureNone = Option.empty.async
  private val futureSeq = Seq.empty.async
  private val futureNil = Nil.async


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

  val unit: Async[Unit] = ().async

  def none[T]: Async[Option[T]] = futureNone

  def seq[T]: Async[Seq[T]] = futureSeq

  def nil[T]: Async[List[T]] = futureNil

  def fold[T, S](iter: Iterable[Async[T]], s: S)(f: (S, T) => S): Async[S] = {

    val iterator = iter.iterator

    @tailrec
    def fold(s: S): Async[S] = {
      if (iterator.isEmpty) Async(s)
      else {
        val v = iterator.next()
        v match {
          case Succeed(v)        => fold(f(s, v))
          case Failed(v)         => Failed(v)
          case v: InCompleted[T] => v.value() match {
            case Some(Success(v)) => fold(f(s, v))
            case Some(Failure(v)) => Failed(v)
            case None             => v.flatMap(v => break(f(s, v)))
          }
        }
      }
    }

    def break(s: S) = fold(s)

    fold(s)
  }


  def foldUnit[T](iter: Iterable[Async[T]]): Async[Unit] = fold(iter, ()) { (_, _) => () }


  sealed trait Completed[+T] extends Async[T] {

    def valueTry: Try[T]

    final def value() = Some(valueTry)

    final def mapTry[TT](f: Try[T] => Try[TT]): Async[TT] = safe { Completed(f(valueTry)) }

    def future = Future.fromTry(valueTry)

    def onComplete[TT](f: Try[T] => TT): Unit = safeUnit { f(valueTry) }

    final def await(timeout: Duration) = this
  }

  object Completed {
    def apply[T](value: Try[T]): Completed[T] = {
      value match {
        case Success(value) => Succeed(value)
        case Failure(value) => Failed(value)
      }
    }
  }


  final case class Succeed[T] private(v: T) extends Completed[T] {

    def valueTry = Success(v)

    def foreach[TT](f: T => TT) = safeUnit { f(v) }

    def map[TT](f: T => TT) = safe { Succeed(f(v)) }

    def flatMap[TT](f: T => Async[TT]) = safe { f(v) }

    def get(timeout: Duration) = v

    def recover[TT >: T](pf: PartialFunction[Throwable, TT]) = this

    override def toString = s"Async($v)"
  }


  final case class Failed private(v: Throwable) extends Completed[Nothing] {

    def valueTry = Failure(v)

    def foreach[TT](f: Nothing => TT) = {}

    def map[TT](f: Nothing => TT) = this

    def flatMap[TT](f: Nothing => Async[TT]) = this

    def get(timeout: Duration) = throw v

    def recover[TT >: Nothing](pf: PartialFunction[Throwable, TT]) = {
      safe { if (pf.isDefinedAt(v)) Succeed(pf(v)) else this }
    }

    override def toString = s"Async($v)"
  }


  final case class InCompleted[T] private(v: Future[T])(implicit ec: ExecutionContext) extends Async[T] {

    def foreach[TT](f: T => TT) = {
      v.value match {
        case Some(Success(v)) => safeUnit { f(v) }
        case Some(Failure(v)) =>
        case None             => v.foreach(f)
      }
    }

    def map[TT](f: T => TT) = {
      v.value match {
        case Some(Success(v)) => safe { Succeed(f(v)) }
        case Some(Failure(v)) => Failed(v)
        case None             => InCompleted(v.map(f))
      }
    }

    def mapTry[TT](f: Try[T] => Try[TT]): Async[TT] = {
      v.value match {
        case Some(v) => safe { Completed(f(v)) }
        case None    => InCompleted(v.transform(f))
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

    def value() = v.value

    def future = v

    def get(timeout: Duration) = Await.result(v, timeout)

    def await(timeout: Duration) = {
      v.value match {
        case Some(Success(v)) => Succeed(v)
        case Some(Failure(v)) => Failed(v)
        case None             => Completed(Try(Await.result(v, timeout)))
      }
    }

    def onComplete[TT](f: Try[T] => TT): Unit = {
      v.value match {
        case Some(v) => safeUnit { f(v) }
        case None    => future.onComplete(f)
      }
    }

    def recover[TT >: T](pf: PartialFunction[Throwable, TT]) = {
      v.value match {
        case Some(Success(v)) => Succeed(v)
        case Some(Failure(v)) => safe { if (pf.isDefinedAt(v)) Succeed(pf(v)) else Failed(v) }
        case None             => InCompleted(v.recover(pf))
      }
    }

    override def toString = "Async(<not completed>)"
  }


  private def safe[T](f: => Async[T]): Async[T] = try f catch { case NonFatal(failure) => Failed(failure) }

  private def safeUnit[T](f: => T): Unit = try f catch { case NonFatal(_) => }
}


object AsyncConverters {
  implicit class FutureAsync[T](val self: Future[T]) extends AnyVal {
    def async(implicit ec: ExecutionContext): Async[T] = Async(self)
  }

  implicit class AnyAsync[T](val self: T) extends AnyVal {
    def async: Async[T] = Async(self)
  }
}