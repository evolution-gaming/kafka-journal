package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Show
import cats.kernel.{Eq, Order}
import cats.syntax.all._
import com.datastax.driver.core.SettableData
import com.evolutiongaming.kafka.journal.util.TemporalHelper._
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.scassandra.{DecodeByName, EncodeByName, EncodeRow}

import java.time.LocalDate

final case class ExpireOn(value: LocalDate) {

  override def toString: String = value.toString
}

object ExpireOn {

  implicit val eqExpireOn: Eq[ExpireOn] = Eq.fromUniversalEquals

  implicit val showExpireOn: Show[ExpireOn] = Show.fromToString

  implicit val orderingExpireOn: Ordering[ExpireOn] = (a: ExpireOn, b: ExpireOn) => a.value compare b.value

  implicit val orderExpireOn: Order[ExpireOn] = Order.fromOrdering

  implicit val encodeByNameExpireOn: EncodeByName[ExpireOn] = EncodeByName[LocalDate].contramap { (a: ExpireOn) => a.value }

  implicit val decodeByNameExpireOn: DecodeByName[ExpireOn] = DecodeByName[LocalDate].map { a => ExpireOn(a) }

  implicit val encodeRowExpireOn: EncodeRow[ExpireOn] = new EncodeRow[ExpireOn] {
    def apply[B <: SettableData[B]](data: B, a: ExpireOn) = {
      data.encode("expire_on", a)
    }
  }

  object implicits {

    implicit class LocalDateOpsExpireOn(val self: LocalDate) extends AnyVal {

      def toExpireOn: ExpireOn = ExpireOn(self)
    }
  }
}
