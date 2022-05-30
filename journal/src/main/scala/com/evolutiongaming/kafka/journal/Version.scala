package com.evolutiongaming.kafka.journal

import cats.Show
import cats.kernel.{Eq, Order}
import com.evolutiongaming.scassandra.{DecodeByName, DecodeRow, EncodeByName, EncodeRow}
import play.api.libs.json.{Reads, Writes}

final case class Version(value: String) {
  override def toString = value
}

object Version {

  val current: Version = Version("1.0.3")


  implicit val eqVersion: Eq[Version] = Eq.fromUniversalEquals

  implicit val showVersion: Show[Version] = Show.fromToString


  implicit val orderingVersion: Ordering[Version] = (a: Version, b: Version) => a.value compare b.value

  implicit val orderVersion: Order[Version] = Order.fromOrdering


  implicit val writesVersion: Writes[Version] = Writes.of[String].contramap(_.value)

  implicit val readsVersion: Reads[Version] = Reads.of[String].map(Version(_))


  implicit val encodeByNameVersion: EncodeByName[Version] = EncodeByName[String].contramap { (a: Version) => a.value }

  implicit val decodeByNameVersion: DecodeByName[Version] = DecodeByName[String].map { a => Version(a) }


  implicit val encodeByNameOptVersion: EncodeByName[Option[Version]] = EncodeByName.optEncodeByName

  implicit val decodeByNameOptVersion: DecodeByName[Option[Version]] = DecodeByName.optDecodeByName


  implicit val encodeRowVersion: EncodeRow[Version] = EncodeRow("version")

  implicit val decodeRowVersion: DecodeRow[Version] = DecodeRow("version")


  implicit val encodeRowOptVersion: EncodeRow[Option[Version]] = EncodeRow("version")

  implicit val decodeRowOptVersion: DecodeRow[Option[Version]] = DecodeRow("version")
}
