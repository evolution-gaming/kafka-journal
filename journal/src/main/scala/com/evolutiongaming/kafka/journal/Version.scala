package com.evolutiongaming.kafka.journal

import cats.Show
import cats.kernel.{Eq, Order}
import play.api.libs.json.{Format, JsString, JsValue}

final case class Version(value: String) {
  override def toString = value
}

object Version {

  val current: Version = Version("0.0.153")

  implicit val formatVersion: Format[Version] = new Format[Version] {

    def writes(version: Version) = JsString(version.value)

    def reads(jsValue: JsValue) = {
      jsValue
        .validate[String]
        .map { a => Version(a) }
    }
  }

  implicit val eqVersion: Eq[Version] = Eq.fromUniversalEquals

  implicit val showVersion: Show[Version] = Show.fromToString


  implicit val orderingVersion: Ordering[Version] = (a: Version, b: Version) => a.value compare b.value

  implicit val orderVersion: Order[Version] = Order.fromOrdering
}
