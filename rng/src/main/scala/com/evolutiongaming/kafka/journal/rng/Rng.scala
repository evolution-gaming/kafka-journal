package com.evolutiongaming.kafka.journal.rng

import cats.FlatMap
import cats.effect.Clock
import cats.implicits._
import com.evolutiongaming.kafka.journal.ClockHelper._

trait Rng {

  def int: (Int, Rng)

  def long: (Long, Rng)

  def float: (Float, Rng)

  def double: (Double, Rng)
}

object Rng {

  type Seed = Long


  def apply(seed: Seed): Rng = Simple(seed)


  def fromClock[F[_] : Clock : FlatMap]: F[Rng] = {
    for {
      nanos <- Clock[F].nanos
    } yield {
      apply(nanos)
    }
  }

  
  private final case class Simple(seed: Seed) extends Rng {

    import Simple._

    private def next(bits: Int, seed: Seed): (Int, Seed) = {
      val r = (seed >>> (48 - bits)).toInt
      val s1 = (seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1)
      (r, s1)
    }

    def int = {
      val (r, s1) = next(32, seed)
      (r, Simple(s1))
    }

    def long = {
      val (a0, s1) = next(32, seed)
      val a1 = a0.toLong << 32
      val (a2, s2) = next(32, s1)
      val r = a1 + a2
      (r, Simple(s2))
    }

    def float = {
      val (a0, s1) = next(24, seed)
      val a = a0 / FloatUnit
      (a, Simple(s1))
    }

    def double = {
      val (a0, s1) = next(26, seed)
      val a1 = a0.toLong << 27
      val (a2, s2) = next(27, s1)
      val r = (a1 + a2) * DoubleUnit
      (r, Simple(s2))
    }
  }

  private object Simple {

    val DoubleUnit = 1.0 / (1L << 53)
    
    val FloatUnit = (1 << 24).toFloat
  }
}