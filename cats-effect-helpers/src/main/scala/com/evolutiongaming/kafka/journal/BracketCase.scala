package com.evolutiongaming.kafka.journal

import cats.effect.ExitCase

import scala.util.{Failure, Success, Try}

trait BracketCase[F[_]] {

  def bracketCase[A, B](acquire: F[A])(use: A => F[B])(release: (A, ExitCase[Throwable]) => F[Unit]): F[B]
}

object BracketCase {

  def apply[F[_]](implicit F: BracketCase[F]): BracketCase[F] = F


  implicit val bracketCaseTry: BracketCase[Try] = new BracketCase[Try] {

    def bracketCase[A, B](acquire: Try[A])(use: A => Try[B])(release: (A, ExitCase[Throwable]) => Try[Unit]) = {

      def exitCase[T](a: Try[T]) = a match {
        case Success(_) => ExitCase.complete[Throwable]
        case Failure(e) => ExitCase.error(e)
      }

      for {
        a <- acquire
        b = use(a)
        ec = exitCase(b)
        _ <- release(a, ec)
        b <- b
      } yield b
    }
  }
}