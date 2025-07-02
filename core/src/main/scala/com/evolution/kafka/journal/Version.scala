package com.evolution.kafka.journal

import cats.Show
import cats.kernel.{Eq, Order}
import play.api.libs.json.{Reads, Writes}

/**
 * Version of Kafka Journal library used to persist an event or a snapshot
 */
final case class Version(value: String) {
  override def toString = value
}

object Version {

  val current: Version = Version(Option(Version.getClass.getPackage.getImplementationVersion).getOrElse("unknown"))

  implicit val eqVersion: Eq[Version] = Eq.fromUniversalEquals

  implicit val showVersion: Show[Version] = Show.fromToString

  implicit val orderingVersion: Ordering[Version] = (a: Version, b: Version) => a.value compare b.value

  implicit val orderVersion: Order[Version] = Order.fromOrdering

  implicit val writesVersion: Writes[Version] = Writes.of[String].contramap(_.value)

  implicit val readsVersion: Reads[Version] = Reads.of[String].map(Version(_))

}
