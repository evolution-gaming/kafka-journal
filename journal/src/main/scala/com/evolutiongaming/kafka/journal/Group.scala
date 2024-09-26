package com.evolutiongaming.kafka.journal

import cats.Defer
import cats.data.NonEmptyList as Nel
import cats.effect.kernel.{Async, Ref}
import cats.effect.syntax.all.*
import cats.effect.{Deferred, Resource}
import cats.implicits.*

import scala.util.control.NoStackTrace

trait Group[F[_], A, B] {
  def apply(a: A): F[B]
}

object Group {

  def make[F[_]: Async, A, B](func: F[Nel[A] => F[B]]): Resource[F, Group[F, A, B]] = {

    sealed trait S

    object S {
      final case object Idle extends S
      final case class Busy(as: List[A], deferred: Deferred[F, Either[Throwable, B]]) extends S
      final case class Releasing(as: List[A], deferred: Deferred[F, Either[Throwable, B]]) extends S
      final case object Released extends S
    }

    Resource
      .make {
        Ref[F].of[S](S.Idle)
      } { ref =>
        ref.modify {
          case S.Idle               => (S.Released, ().pure[F])
          case S.Busy(as, deferred) => (S.Releasing(as, deferred), deferred.get.void)
          case s: S.Releasing       => (s, s.deferred.get.void)
          case s @ S.Released       => (s, ().pure[F])
        }.flatten
      }
      .map { ref =>
        def start: F[Unit] = {
          val stop = ().asRight[Int]
          0
            .tailRecM { count =>
              def continue = (count + 1).asLeft[Unit]

              func
                .attempt
                .flatMap { func =>
                  def run(a: A, as: List[A], deferred: Deferred[F, Either[Throwable, B]]) = {
                    Defer[F].defer {
                      for {
                        b <- func
                          .liftTo[F]
                          .flatMap { _.apply(Nel(a, as)) }
                          .attempt
                        _ <- deferred
                          .complete(b)
                          .void // cats-effect-3
                        a <- ref.modify {
                          case s @ S.Idle          => (s, stop)
                          case S.Busy(Nil, _)      => (S.Idle, stop)
                          case s: S.Busy           => (s, continue)
                          case S.Releasing(Nil, _) => (S.Released, stop)
                          case s: S.Releasing      => (s, continue)
                          case s @ S.Released      => (s, stop)
                        }
                      } yield a
                    }
                  }

                  ref.modify {
                    case state @ S.Idle                 => (state, stop.pure[F])
                    case S.Busy(a :: as, deferred)      => (S.Busy(Nil, deferred), run(a, as, deferred))
                    case S.Busy(Nil, _)                 => (S.Idle, stop.pure[F])
                    case S.Releasing(a :: as, deferred) => (S.Releasing(Nil, deferred), run(a, as, deferred))
                    case S.Releasing(Nil, _)            => (S.Released, stop.pure[F])
                    case state @ S.Released             => (state, stop.pure[F])
                  }.flatten
                }
            }
            .start
            .void
        }

        class Main
        new Main with Group[F, A, B] {
          def apply(a: A) = {
            ref
              .modify {
                case S.Idle =>
                  val deferred = Deferred.unsafe[F, Either[Throwable, B]]
                  (S.Busy(a :: Nil, deferred), Defer[F].defer { start.as(deferred) })

                case S.Busy(Nil, _) =>
                  val deferred = Deferred.unsafe[F, Either[Throwable, B]]
                  (S.Busy(a :: Nil, deferred), deferred.pure[F])

                case S.Busy(as, deferred) =>
                  (S.Busy(a :: as, deferred), deferred.pure[F])

                case s: S.Releasing =>
                  (s, ReleasedError.raiseError[F, Deferred[F, Either[Throwable, B]]])

                case s @ S.Released =>
                  (s, ReleasedError.raiseError[F, Deferred[F, Either[Throwable, B]]])
              }
              .flatten
              .uncancelable
              .flatMap { _.get }
              .rethrow
          }
        }
      }
  }

  final case object ReleasedError extends RuntimeException("released") with NoStackTrace
}
