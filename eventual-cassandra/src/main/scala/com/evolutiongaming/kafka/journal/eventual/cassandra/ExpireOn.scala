package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.LocalDate

import cats.Show
import cats.implicits._
import cats.kernel.{Eq, Order}
import com.evolutiongaming.kafka.journal.util.TemporalHelper._

final case class ExpireOn(value: LocalDate) {

  override def toString: String = value.toString
}

object ExpireOn {

  implicit val eqExpireOn: Eq[ExpireOn] = Eq.fromUniversalEquals

  implicit val showExpireOn: Show[ExpireOn] = Show.fromToString


  implicit val orderingExpireOn: Ordering[ExpireOn] = (a: ExpireOn, b: ExpireOn) => a.value compare b.value

  implicit val orderExpireOn: Order[ExpireOn] = Order.fromOrdering


  object implicits {

    implicit class LocalDateOpsExpireOn(val self: LocalDate) extends AnyVal {

      def toExpireOn: ExpireOn = ExpireOn(self)
    }
  }
}
