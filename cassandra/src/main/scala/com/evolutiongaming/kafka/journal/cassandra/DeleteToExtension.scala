package com.evolutiongaming.kafka.journal.cassandra

import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.cassandra.SeqNrExtension.*
import com.evolutiongaming.kafka.journal.{DeleteTo, SeqNr}
import com.evolutiongaming.scassandra.*

object DeleteToExtension {
  implicit val encodeByNameDeleteTo: EncodeByName[DeleteTo] = EncodeByName[SeqNr].contramap { (a: DeleteTo) => a.value }

  implicit val decodeByNameDeleteTo: DecodeByName[DeleteTo] = DecodeByName[SeqNr].map { _.toDeleteTo }

  implicit val encodeByIdxDeleteTo: EncodeByIdx[DeleteTo] = EncodeByIdx[SeqNr].contramap { (a: DeleteTo) => a.value }

  implicit val decodeByIdxDeleteTo: DecodeByIdx[DeleteTo] = DecodeByIdx[SeqNr].map { _.toDeleteTo }

  implicit val encodeByNameOptDeleteTo: EncodeByName[Option[DeleteTo]] = EncodeByName.optEncodeByName[DeleteTo]

  implicit val decodeByNameOptDeleteTo: DecodeByName[Option[DeleteTo]] = DecodeByName.optDecodeByName[DeleteTo]

  implicit val encodeRowDeleteTo: EncodeRow[DeleteTo] = EncodeRow[DeleteTo]("delete_to")

  implicit val decodeRowDeleteTo: DecodeRow[DeleteTo] = DecodeRow[DeleteTo]("delete_to")
}
