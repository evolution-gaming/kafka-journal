package com.evolution.kafka.journal.cassandra

import cats.Id
import com.evolution.kafka.journal.BufferNr
import com.evolutiongaming.scassandra.*

private[journal] object BufferNrExtension {
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
