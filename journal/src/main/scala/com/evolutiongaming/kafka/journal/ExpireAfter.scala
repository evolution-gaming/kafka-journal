package com.evolutiongaming.kafka.journal

import cats.Show
import cats.kernel.Order
import com.evolutiongaming.kafka.journal.util.PlayJsonHelper._
import play.api.libs.json.{Reads, Writes}

import scala.concurrent.duration.FiniteDuration

final case class ExpireAfter(duration: FiniteDuration) {

  override def toString: String = duration.toString()
}

object ExpireAfter {

  implicit val showExpireAfter: Show[ExpireAfter] = Show.fromToString


  implicit val orderingExpireAfter: Ordering[ExpireAfter] = (a: ExpireAfter, b: ExpireAfter) => a.duration compare b.duration

  implicit val orderExpireAfter: Order[ExpireAfter] = Order.fromOrdering


  implicit val writesExpireAfter: Writes[ExpireAfter] = Writes.of[FiniteDuration].contramap { _.duration }

  implicit val readsExpireAfter: Reads[ExpireAfter] = Reads.of[FiniteDuration].map { a => ExpireAfter(a) }


  object implicits {

    implicit class FiniteDurationOpsExpireAfter(val self: FiniteDuration) extends AnyVal {

      def toExpireAfter: ExpireAfter = ExpireAfter(self)
    }
  }
}
