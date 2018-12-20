package com.evolutiongaming.kafka.journal.util

import cats.FlatMap
import cats.effect.Clock
import cats.implicits._
import com.evolutiongaming.kafka.journal.util.ClockHelper._

trait Rng {

  def int: (Int, Rng)

  def double: (Double, Rng)
}

object Rng {

  type Seed = Long

  private val DoubleUnit = 1.0 / (1L << 53)


  def apply(seed: Seed): Rng = Simple(seed)


  def fromClock[F[_] : Clock : FlatMap]: F[Rng] = {
    for {
      nanos <- ClockOf[F].nanos
    } yield {
      apply(nanos)
    }
  }

  private final case class Simple(seed: Seed) extends Rng {

    private def next(bits: Int, seed: Seed): (Int, Seed) = {
      val r = (seed >>> (48 - bits)).toInt
      val s1 = (seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1)
      (r, s1)
    }

    def double = {
      val (a0, s1) = next(26, seed)
      val a1 = a0.toLong << 27
      val (a2, s2) = next(27, s1)
      val r = (a1 + a2) * DoubleUnit
      (r, Simple(s2))
    }

    def int = {
      val (r, s1) = next(32, seed)
      (r, Simple(s1))
    }
  }
}