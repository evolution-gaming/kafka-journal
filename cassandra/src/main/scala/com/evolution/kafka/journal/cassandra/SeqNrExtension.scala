package com.evolution.kafka.journal.cassandra

import cats.Id
import cats.syntax.all.*
import com.evolution.kafka.journal.SeqNr
import com.evolutiongaming.scassandra.*

object SeqNrExtension {
  implicit val encodeByNameSeqNr: EncodeByName[SeqNr] = EncodeByName[Long].contramap((seqNr: SeqNr) => seqNr.value)

  implicit val decodeByNameSeqNr: DecodeByName[SeqNr] = DecodeByName[Long].map(value => SeqNr.of[Id](value))

  implicit val encodeByIdxSeqNr: EncodeByIdx[SeqNr] = EncodeByIdx[Long].contramap((seqNr: SeqNr) => seqNr.value)

  implicit val decodeByIdxSeqNr: DecodeByIdx[SeqNr] = DecodeByIdx[Long].map(value => SeqNr.of[Id](value))

  implicit val encodeByNameOptSeqNr: EncodeByName[Option[SeqNr]] = EncodeByName.optEncodeByName[SeqNr]

  implicit val decodeByNameOptSeqNr: DecodeByName[Option[SeqNr]] = DecodeByName[Option[Long]].map { value =>
    for {
      a <- value
      a <- SeqNr.opt(a)
    } yield a
  }

  implicit val encodeRowSeqNr: EncodeRow[SeqNr] = EncodeRow[SeqNr]("seq_nr")

  implicit val decodeRowSeqNr: DecodeRow[SeqNr] = DecodeRow[SeqNr]("seq_nr")
}
