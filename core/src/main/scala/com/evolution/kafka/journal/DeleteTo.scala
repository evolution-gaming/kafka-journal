package com.evolution.kafka.journal

import cats.syntax.all.*
import cats.{Eq, Order, Show}
import play.api.libs.json.*

final case class DeleteTo(value: SeqNr) {

  override def toString: String = value.toString
}

object DeleteTo {

  val min: DeleteTo = SeqNr.min.toDeleteTo

  val max: DeleteTo = SeqNr.max.toDeleteTo

  implicit val eqDeleteTo: Eq[DeleteTo] = Eq.fromUniversalEquals

  implicit val showDeleteTo: Show[DeleteTo] = Show.fromToString

  implicit val orderingDeleteTo: Ordering[DeleteTo] = (a: DeleteTo, b: DeleteTo) => a.value compare b.value

  implicit val orderDeleteTo: Order[DeleteTo] = Order.fromOrdering

  implicit val writesDeleteTo: Writes[DeleteTo] = Writes.of[SeqNr].contramap { _.value }

  implicit val readsDeleteTo: Reads[DeleteTo] = Reads.of[SeqNr].map { _.toDeleteTo }
}
