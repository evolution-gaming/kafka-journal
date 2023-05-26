package com.evolutiongaming.kafka.journal.util

import cats.effect.implicits.effectResourceOps
import cats.effect.kernel.{DeferredSource, RefSource}
import cats.effect.syntax.all._
import cats.effect.{Deferred, Ref, Resource, Temporal}
import cats.syntax.all._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.retry.Retry.implicits._
import com.evolutiongaming.retry.{OnError, Retry, Strategy}

import scala.concurrent.duration._

trait ExpiringRef[F[_], A] extends RefSource[F, A] {
  def tryGet: F[Option[A]]
}

object ExpiringRef {

  def of[F[_]: Temporal: Log, A](
    load: F[A],
    expireIn: FiniteDuration,
    strategy: Strategy = Strategy.const(0.seconds).attempts(10),
  ): Resource[F, ExpiringRef[F, A]] = {

    type E = Either[Throwable, A]

    implicit val retry = Retry(strategy, OnError.fromLog(Log[F]))

    def completed(e: E): DeferredSource[F, E] =
      new DeferredSource[F, E] {
        val get: F[E] = e.pure[F]
        val tryGet: F[Option[E]] = e.some.pure[F]
      }

    for {
      d <- Deferred[F, E].toResource
      r <- Ref.of[F, DeferredSource[F, E]](d).toResource

      f = for {
        e <- load.retry.attempt
        _ <- d.complete(e)
      } yield {}
      _ <- f.start.toResource

      s = for {
        _ <- Temporal[F].sleep(expireIn)
        e <- load.retry.attempt
        _ <- r.set(completed(e))
      } yield {}
      _ <- s.foreverM[Unit].background
    } yield new ExpiringRef[F, A] {

      override def get: F[A] =
        for {
          d <- r.get
          e <- d.get
          a <- e.liftTo[F]
        } yield a

      override def tryGet: F[Option[A]] =
        for {
          d <- r.get
          o <- d.tryGet
        } yield for {
          e <- o
          a <- e.toOption
        } yield a

    }
  }

}
