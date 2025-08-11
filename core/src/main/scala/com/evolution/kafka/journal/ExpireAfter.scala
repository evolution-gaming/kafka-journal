package com.evolution.kafka.journal

import cats.{Eq, Order, Show}
import com.evolution.kafka.journal.util.PlayJsonHelper.*
import play.api.libs.json.{Reads, Writes}

import scala.concurrent.duration.FiniteDuration

final case class ExpireAfter(value: FiniteDuration) {

  override def toString: String = value.toString()
}

object ExpireAfter {

  implicit val eqExpireAfter: Eq[ExpireAfter] = Eq.fromUniversalEquals

  implicit val showExpireAfter: Show[ExpireAfter] = Show.fromToString

  implicit val orderingExpireAfter: Ordering[ExpireAfter] = (a, b) => a.value compare b.value

  implicit val orderExpireAfter: Order[ExpireAfter] = Order.fromOrdering

  implicit val writesExpireAfter: Writes[ExpireAfter] = Writes.of[FiniteDuration].contramap { _.value }

  implicit val readsExpireAfter: Reads[ExpireAfter] = Reads.of[FiniteDuration].map { a => ExpireAfter(a) }

  object implicits {

    implicit class FiniteDurationOpsExpireAfter(val self: FiniteDuration) extends AnyVal {

      def toExpireAfter: ExpireAfter = ExpireAfter(self)
    }
  }
}
