package com.evolutiongaming.kafka.journal

import cats.kernel.Order
import cats.syntax.all._
import cats.{Eq, Show}
import com.evolutiongaming.scassandra._
import play.api.libs.json._

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

  implicit val encodeByNameDeleteTo: EncodeByName[DeleteTo] = EncodeByName[SeqNr].contramap { (a: DeleteTo) => a.value }

  implicit val decodeByNameDeleteTo: DecodeByName[DeleteTo] = DecodeByName[SeqNr].map { _.toDeleteTo }

  implicit val encodeByIdxDeleteTo: EncodeByIdx[DeleteTo] = EncodeByIdx[SeqNr].contramap { (a: DeleteTo) => a.value }

  implicit val decodeByIdxDeleteTo: DecodeByIdx[DeleteTo] = DecodeByIdx[SeqNr].map { _.toDeleteTo }

  implicit val encodeByNameOptDeleteTo: EncodeByName[Option[DeleteTo]] = EncodeByName.optEncodeByName[DeleteTo]

  implicit val decodeByNameOptDeleteTo: DecodeByName[Option[DeleteTo]] = DecodeByName.optDecodeByName[DeleteTo]

  implicit val encodeRowDeleteTo: EncodeRow[DeleteTo] = EncodeRow[DeleteTo]("delete_to")

  implicit val decodeRowDeleteTo: DecodeRow[DeleteTo] = DecodeRow[DeleteTo]("delete_to")

  implicit val writesDeleteTo: Writes[DeleteTo] = Writes.of[SeqNr].contramap { _.value }

  implicit val readsDeleteTo: Reads[DeleteTo] = Reads.of[SeqNr].map { _.toDeleteTo }
}
