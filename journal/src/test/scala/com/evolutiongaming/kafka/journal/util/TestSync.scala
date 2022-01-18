package com.evolutiongaming.kafka.journal.util

import cats.Monad
import cats.effect.Sync
import cats.effect.kernel.{CancelScope, Poll}

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

object TestSync {

  def apply[F[_]](implicit F: Monad[F]): Sync[F] = new Sync[F] {
    override def suspend[A](hint: Sync.Type)(thunk: => A): F[A] = F.pure(thunk)

    override def monotonic: F[FiniteDuration] = ???

    override def realTime: F[FiniteDuration] = ???

    override def rootCancelScope: CancelScope = CancelScope.Uncancelable

    override def forceR[A, B](fa: F[A])(fb: F[B]): F[B] = {
      try { fa } catch { case NonFatal(_) => () }
      fb
    }

    override def uncancelable[A](body: Poll[F] => F[A]): F[A] = body(new Poll[F] {
      override def apply[X](fa: F[X]): F[X] = fa
    })

    override def canceled: F[Unit] = F.pure(())

    override def onCancel[A](fa: F[A], fin: F[Unit]): F[A] = fa

    override def raiseError[A](e: Throwable): F[A] = throw e

    override def handleErrorWith[A](fa: F[A])(f: Throwable => F[A]): F[A] =
      try { fa } catch { case NonFatal(e) => f(e) }

    override def flatMap[A, B](fa: F[A])(f: A => F[B]): F[B] = F.flatMap(fa)(f)

    override def tailRecM[A, B](a: A)(f: A => F[Either[A, B]]): F[B] = F.tailRecM(a)(f)

    override def pure[A](x: A): F[A] = F.pure(x)
  }
}
