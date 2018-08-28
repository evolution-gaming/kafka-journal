package com.evolutiongaming.kafka.journal

import scala.concurrent.Future

trait AdaptFuture[F[_]] {
  def apply[A](future: Future[A]): F[A]
}

object AdaptFuture {
  def apply[F[_]](implicit F: AdaptFuture[F]): AdaptFuture[F] = F
}
