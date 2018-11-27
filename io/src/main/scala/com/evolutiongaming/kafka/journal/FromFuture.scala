package com.evolutiongaming.kafka.journal

import cats.arrow.FunctionK

import scala.concurrent.Future

trait FromFuture[F[_]] extends FunctionK[Future, F]

object FromFuture {

  def apply[F[_]](implicit f: FromFuture[F]): FromFuture[F] = f
}
