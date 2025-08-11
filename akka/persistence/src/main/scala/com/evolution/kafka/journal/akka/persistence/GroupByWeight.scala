package com.evolution.kafka.journal.akka.persistence

import cats.syntax.all.*

private[journal] object GroupByWeight {

  def apply[A](as: List[A], maxWeight: Int)(weight: A => Int): List[List[A]] = {

    def weightOf(as: List[A]) = as.foldLeft(0) { _ + weight(_) }

    if (as.isEmpty) Nil
    else if (maxWeight <= 0) List(as)
    else if (maxWeight === 1) as.map(List(_))
    else if (weightOf(as) <= maxWeight) List(as)
    else {
      val bs = as.foldLeft(List.empty[List[A]]) { (bs, a) =>
        bs match {
          case Nil => List(List(a))
          case b :: bs =>
            if (weight(a) + weightOf(b) <= maxWeight) (a :: b) :: bs
            else List(a) :: b :: bs
        }
      }
      bs.foldLeft(List.empty[List[A]]) { (a, b) => b.reverse :: a }
    }
  }
}
