package com.evolutiongaming.kafka.journal

import cats.arrow.FunctionK

import scala.concurrent.Future

trait FromFuture2[F[_]] extends FunctionK[Future, F]

object FromFuture2 {

  def apply[F[_]](implicit f: FromFuture2[F]): FromFuture2[F] = f
}
