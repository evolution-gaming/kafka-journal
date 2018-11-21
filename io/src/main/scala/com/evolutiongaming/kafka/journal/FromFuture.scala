package com.evolutiongaming.kafka.journal

import cats.arrow.FunctionK

import scala.concurrent.Future

trait FromFuture[F[_]] extends FunctionK[Future, F] {
  def apply[A](fa: Future[A]): F[A]
}

object FromFuture {

  def apply[F[_]](implicit f: FromFuture[F]): FromFuture[F] = f
}
