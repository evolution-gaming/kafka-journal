package com.evolutiongaming.kafka.journal

import com.evolutiongaming.scassandra.DecodeByIdx
import com.evolutiongaming.scassandra.DecodeByName
import com.evolutiongaming.scassandra.DecodeRow
import com.evolutiongaming.scassandra.EncodeByIdx
import com.evolutiongaming.scassandra.EncodeByName
import com.evolutiongaming.scassandra.EncodeRow

sealed abstract case class BufferNr(value: Int) {
  override def toString: String = value.toString
}

object BufferNr {

  def listOf(size: Int): List[BufferNr] =
    (0 until size).toList.map(fromIntUnsafe)

  def fromIntUnsafe(value: Int): BufferNr =
    new BufferNr(value) {}

  implicit val encodeByNameBufferNr: EncodeByName[BufferNr] =
    EncodeByName[Int].contramap(_.value)
  implicit val decodeByNameBufferNr: DecodeByName[BufferNr] =
    DecodeByName[Int].map(fromIntUnsafe)

  implicit val encodeByIdxBufferNr: EncodeByIdx[BufferNr] =
    EncodeByIdx[Int].contramap(_.value)
  implicit val decodeByIdxBufferNr: DecodeByIdx[BufferNr] =
    DecodeByIdx[Int].map(fromIntUnsafe)

  implicit val encodeRowSeqNr: EncodeRow[BufferNr] =
    EncodeRow[BufferNr]("buffer_idx")
  implicit val decodeRowSeqNr: DecodeRow[BufferNr] =
    DecodeRow[BufferNr]("buffer_idx")

}
