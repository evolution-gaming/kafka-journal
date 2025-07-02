package com.evolution.kafka.journal.cassandra

import com.evolution.kafka.journal.Origin
import com.evolutiongaming.scassandra.{DecodeByName, DecodeRow, EncodeByName, EncodeRow}

object OriginExtension {
  implicit val encodeByNameOrigin: EncodeByName[Origin] = EncodeByName[String].contramap((a: Origin) => a.value)

  implicit val decodeByNameOrigin: DecodeByName[Origin] = DecodeByName[String].map(a => Origin(a))

  implicit val encodeByNameOptOrigin: EncodeByName[Option[Origin]] = EncodeByName.optEncodeByName

  implicit val decodeByNameOptOrigin: DecodeByName[Option[Origin]] = DecodeByName.optDecodeByName

  implicit val encodeRowOrigin: EncodeRow[Origin] = EncodeRow("origin")

  implicit val decodeRowOrigin: DecodeRow[Origin] = DecodeRow("origin")

  implicit val encodeRowOptOrigin: EncodeRow[Option[Origin]] = EncodeRow("origin")

  implicit val decodeRowOptOrigin: DecodeRow[Option[Origin]] = DecodeRow("origin")
}
