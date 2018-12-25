package com.evolutiongaming.kafka.journal.util

import cats.effect.IO

import scala.concurrent.Future

trait ToFuture[F[_]] {
  def apply[A](f: F[A]): Future[A]
}

object ToFuture {

  def apply[F[_]](implicit f: ToFuture[F]): ToFuture[F] = f

  val io: ToFuture[IO] = new ToFuture[IO] {
    def apply[A](f: IO[A]) = f.unsafeToFuture()
  }

  implicit def ioToFuture: ToFuture[IO] = io
}
