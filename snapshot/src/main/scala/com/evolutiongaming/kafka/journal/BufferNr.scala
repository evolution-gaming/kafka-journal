package com.evolutiongaming.kafka.journal

import cats.syntax.all._
import cats.{Applicative, Id}
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.kafka.journal.util.Fail.implicits._
import com.evolutiongaming.scassandra._

/** Snapshot index in a stored ring buffer */
sealed abstract case class BufferNr(value: Int) {
  override def toString: String = value.toString
}

object BufferNr {

  val min: BufferNr = BufferNr.fromIntUnsafe(0)
  val max: BufferNr = BufferNr.fromIntUnsafe(Int.MaxValue)

  /** Create all list of buffer indicies of a given size.
    *
    * I.e. for `size = 3` the following list will be created:
    * {{{
    * List(BufferNr(0), BufferNr(1), BufferNr(2))
    * }}}
    */
  def listOf(size: Int): List[BufferNr] =
    (0 until size).toList.map(fromIntUnsafe)

  private def fromIntUnsafe(value: Int): BufferNr =
    new BufferNr(value) {}

  /** Create `BufferNr` out of a value or fail it is out of an allowed range.
    *
    * A returned value may be reused to minimize number of allocations.
    */
  def of[F[_]: Applicative: Fail](value: Int): F[BufferNr] = {
    if (value < min.value) {
      s"invalid BufferNr of $value, it must be greater or equal to $min".fail[F, BufferNr]
    } else if (value > max.value) {
      s"invalid BufferNr of $value, it must be less or equal to $max".fail[F, BufferNr]
    } else if (value === min.value) {
      min.pure[F]
    } else if (value === max.value) {
      max.pure[F]
    } else {
      fromIntUnsafe(value).pure[F]
    }
  }

  implicit val encodeByNameBufferNr: EncodeByName[BufferNr] =
    EncodeByName[Int].contramap(_.value)
  implicit val decodeByNameBufferNr: DecodeByName[BufferNr] =
    DecodeByName[Int].map(BufferNr.of[Id])

  implicit val encodeByIdxBufferNr: EncodeByIdx[BufferNr] =
    EncodeByIdx[Int].contramap(_.value)
  implicit val decodeByIdxBufferNr: DecodeByIdx[BufferNr] =
    DecodeByIdx[Int].map(BufferNr.of[Id])

  implicit val encodeRowSeqNr: EncodeRow[BufferNr] =
    EncodeRow[BufferNr]("buffer_idx")
  implicit val decodeRowSeqNr: DecodeRow[BufferNr] =
    DecodeRow[BufferNr]("buffer_idx")

}
