package com.evolutiongaming.kafka.journal

import cats.Show
import cats.implicits._
import cats.kernel.Order
import com.evolutiongaming.scassandra._
import play.api.libs.json._

final case class DeleteTo(seqNr: SeqNr)

object DeleteTo {

  implicit val showDeleteTo: Show[DeleteTo] = Show.fromToString


  implicit val orderingDeleteTo: Ordering[DeleteTo] = (a: DeleteTo, b: DeleteTo) => a.seqNr compare b.seqNr

  implicit val orderDeleteTo: Order[DeleteTo] = Order.fromOrdering


  implicit val encodeByNameDeleteTo: EncodeByName[DeleteTo] = EncodeByName[SeqNr].contramap { (a: DeleteTo) => a.seqNr }

  implicit val decodeByNameDeleteTo: DecodeByName[DeleteTo] = DecodeByName[SeqNr].map { _.toDeleteTo }


  implicit val encodeByIdxDeleteTo: EncodeByIdx[DeleteTo] = EncodeByIdx[SeqNr].contramap { a: DeleteTo => a.seqNr }

  implicit val decodeByIdxDeleteTo: DecodeByIdx[DeleteTo] = DecodeByIdx[SeqNr].map { _.toDeleteTo }


  implicit val encodeByNameOptDeleteTo: EncodeByName[Option[DeleteTo]] = EncodeByName.optEncodeByName[DeleteTo]

  implicit val decodeByNameOptDeleteTo: DecodeByName[Option[DeleteTo]] = DecodeByName.optDecodeByName[DeleteTo]


  implicit val encodeRowDeleteTo: EncodeRow[DeleteTo] = EncodeRow[DeleteTo]("delete_to")

  implicit val decodeRowDeleteTo: DecodeRow[DeleteTo] = DecodeRow[DeleteTo]("delete_to")


  implicit val writesDeleteTo: Writes[DeleteTo] = Writes.of[SeqNr].contramap { _.seqNr }

  implicit val readsDeleteTo: Reads[DeleteTo] = Reads.of[SeqNr].map { _.toDeleteTo }
}