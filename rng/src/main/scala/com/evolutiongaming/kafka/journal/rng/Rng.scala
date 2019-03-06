package com.evolutiongaming.kafka.journal.rng

import cats.effect.Clock
import cats.implicits._
import cats.{FlatMap, Id, ~>}
import com.evolutiongaming.kafka.journal.ClockHelper._

trait Rng[F[_]] {

  def int: F[Int]

  def long: F[Long]

  def float: F[Float]

  def double: F[Double]
}

object Rng {

  type Seed = Long


  implicit class RngOps[F[_]](val self: Rng[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): Rng[G] = new Rng[G] {

      def int = f(self.int)

      def long = f(self.long)

      def float = f(self.float)

      def double = f(self.double)
    }
  }


  type SeedT[A] = cats.data.StateT[Id, Seed, A]

  object SeedT {

    val Rng: Rng[SeedT] = {

      val doubleUnit = 1.0 / (1L << 53)

      val floatUnit = (1 << 24).toFloat

      def next(bits: Int): SeedT[Int] = {
        SeedT { seed =>
          val r = (seed >>> (48 - bits)).toInt
          val s1 = (seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1)
          (s1, r)
        }
      }

      new Rng[SeedT] {

        def int = next(32)

        def long = {
          for {
            a0 <- next(32)
            a1  = a0.toLong << 32
            a2 <- next(32)
          } yield {
            a1 + a2
          }
        }

        def float = {
          for {
            a <- next(24)
          } yield {
            a / floatUnit
          }
        }

        def double = {
          for {
            a0 <- next(26)
            a1  = a0.toLong << 27
            a2 <- next(27)
          } yield {
            (a1 + a2) * doubleUnit
          }
        }
      }
    }


    def apply[A](f: Seed => (Seed, A)): SeedT[A] = cats.data.StateT[Id, Seed, A] { seed => f(seed) }
  }


  final case class State(seed: Seed, rng: Rng[SeedT] = SeedT.Rng) extends Rng[State.Type] {

    private def apply[A](stateT: SeedT[A]) = {
      val (seed1, a) = stateT.run(seed)
      (State(seed1, rng), a)
    }

    def int = apply(rng.int)

    def long = apply(rng.long)

    def float = apply(rng.float)

    def double = apply(rng.double)
  }

  object State { self =>

    type Type[A] = (State, A)

    def fromClock[F[_] : Clock : FlatMap](rng: Rng[SeedT] = SeedT.Rng): F[State] = {
      for {
        nanos <- Clock[F].nanos
      } yield {
        apply(nanos, rng)
      }
    }
  }
}
