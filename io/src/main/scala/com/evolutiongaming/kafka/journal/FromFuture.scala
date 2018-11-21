package com.evolutiongaming.kafka.journal

import scala.concurrent.Future

trait FromFuture[F[_]] {
  def apply[A](fa: => Future[A]): F[A]
}

object FromFuture {

  def apply[F[_]](implicit f: FromFuture[F]): FromFuture[F] = f
}
