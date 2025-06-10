package com.evolutiongaming.kafka.journal.cassandra

import com.evolutiongaming.kafka.journal.Version
import com.evolutiongaming.scassandra.{DecodeByName, DecodeRow, EncodeByName, EncodeRow}

object VersionExtension {
  implicit val encodeByNameVersion: EncodeByName[Version] = EncodeByName[String].contramap { (a: Version) => a.value }

  implicit val decodeByNameVersion: DecodeByName[Version] = DecodeByName[String].map { a => Version(a) }

  implicit val encodeByNameOptVersion: EncodeByName[Option[Version]] = EncodeByName.optEncodeByName

  implicit val decodeByNameOptVersion: DecodeByName[Option[Version]] = DecodeByName.optDecodeByName

  implicit val encodeRowVersion: EncodeRow[Version] = EncodeRow("version")

  implicit val decodeRowVersion: DecodeRow[Version] = DecodeRow("version")

  implicit val encodeRowOptVersion: EncodeRow[Option[Version]] = EncodeRow("version")

  implicit val decodeRowOptVersion: DecodeRow[Option[Version]] = DecodeRow("version")
}
