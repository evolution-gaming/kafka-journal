package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Show
import cats.kernel.Eq
import com.datastax.driver.core.{GettableByNameData, SettableData}
import com.evolutiongaming.kafka.journal.ExpireAfter
import com.evolutiongaming.kafka.journal.ExpireAfter.implicits.*
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper.*
import com.evolutiongaming.scassandra.syntax.*
import com.evolutiongaming.scassandra.{DecodeByName, DecodeRow, EncodeByName, EncodeRow}

import scala.concurrent.duration.FiniteDuration

final case class Expiry(after: ExpireAfter, on: ExpireOn)

object Expiry {

  implicit val eqExpiry: Eq[Expiry] = Eq.fromUniversalEquals

  implicit val showExpiry: Show[Expiry] = Show.fromToString

  implicit val encodeByNameOptExpireOn: EncodeByName[Option[ExpireOn]] = EncodeByName.optEncodeByName[ExpireOn]

  implicit val decodeByNameOptExpireOn: DecodeByName[Option[ExpireOn]] = DecodeByName.optDecodeByName[ExpireOn]

  implicit val encodeByNameExpireAfter: EncodeByName[ExpireAfter] = EncodeByName[FiniteDuration].contramap {
    (a: ExpireAfter) =>
      a.value
  }

  implicit val decodeByNameExpireAfter: DecodeByName[ExpireAfter] = DecodeByName[FiniteDuration].map { _.toExpireAfter }

  implicit val encodeByNameOptExpireAfter: EncodeByName[Option[ExpireAfter]] = EncodeByName.optEncodeByName[ExpireAfter]

  implicit val decodeByNameOptExpireAfter: DecodeByName[Option[ExpireAfter]] = DecodeByName.optDecodeByName[ExpireAfter]

  implicit val decodeRowExpiryOpt: DecodeRow[Option[Expiry]] = { (row: GettableByNameData) =>
    {
      for {
        expireAfter <- row.decode[Option[ExpireAfter]]("expire_after")
        expireOn <- row.decode[Option[ExpireOn]]("expire_on")
      } yield {
        Expiry(expireAfter, expireOn)
      }
    }
  }

  implicit val encodeRowExpiry: EncodeRow[Expiry] = new EncodeRow[Expiry] {
    def apply[B <: SettableData[B]](data: B, a: Expiry) = {
      data
        .encode("expire_after", a.after)
        .encode(a.on)
    }
  }

  implicit val encodeRowExpireOpt: EncodeRow[Option[Expiry]] = EncodeRow.noneAsUnset
}
