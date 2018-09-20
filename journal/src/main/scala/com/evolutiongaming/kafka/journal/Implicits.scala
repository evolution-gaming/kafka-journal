package com.evolutiongaming.kafka.journal

import scala.concurrent.Future

object Implicits {

  implicit class IOOps[A, F[_]](fa: F[A]) {
    def map[B](f: A => B)(implicit F: IO[F]): F[B] = F.map(fa, f)
    def flatMap[B](afb: A => F[B])(implicit F: IO[F]): F[B] = F.flatMap(fa, afb)
    def catchAll[B >: A](ftb: Throwable => F[B])(implicit F: IO[F]): F[B] = F.catchAll(fa, ftb)
  }

  implicit class IOIdOps[A](val self: A) extends AnyVal {
    def pure[F[_] : IO]: F[A] = IO[F].pure(self)
  }

  implicit class FutureIdOps[A](val self: Future[A]) extends AnyVal {
    def adapt[F[_] : AdaptFuture]: F[A] = AdaptFuture[F].apply(self)
  }


  implicit class OptionIdOps[A](val self: A) extends AnyVal {
    def some: Option[A] = Some(self)
  }

  def none[A]: Option[A] = Option.empty

  def unit[F[_] : IO]: F[Unit] = IO[F].unit
}