package com.evolution.kafka.journal

import cats.Applicative
import cats.syntax.all.*
import com.evolution.kafka.journal.util.Fail
import com.evolution.kafka.journal.util.Fail.implicits.*

/**
 * Snapshot index in a stored ring buffer
 */
private[journal] sealed abstract case class BufferNr(value: Int) {
  override def toString: String = value.toString
}

private[journal] object BufferNr {

  val min: BufferNr = BufferNr.fromIntUnsafe(0)
  val max: BufferNr = BufferNr.fromIntUnsafe(Int.MaxValue)

  /**
   * Create a list of N indicies starting from [[BufferNr#min]].
   *
   * I.e. for `size = 3` the following list will be created:
   * {{{
   * List(BufferNr(0), BufferNr(1), BufferNr(2))
   * }}}
   */
  def listOf(size: Int): List[BufferNr] = (0 until size).toList.map(fromIntUnsafe)

  private def fromIntUnsafe(value: Int): BufferNr =
    new BufferNr(value) {}

  /**
   * Create `BufferNr` from a value or fail if it is out of an allowed range.
   *
   * A returned value may be reused to minimize number of allocations.
   */
  def of[F[_]: Applicative: Fail](value: Int): F[BufferNr] = value match {
    case value if value < min.value =>
      s"invalid BufferNr of $value, it must be greater or equal to $min".fail[F, BufferNr]
    case value if value > max.value =>
      s"invalid BufferNr of $value, it must be less or equal to $max".fail[F, BufferNr]
    case min.value => min.pure[F]
    case max.value => max.pure[F]
    case value => fromIntUnsafe(value).pure[F]
  }
}
