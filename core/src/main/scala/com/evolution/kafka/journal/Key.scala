package com.evolution.kafka.journal

import cats.kernel.Eq
import cats.syntax.all.*
import cats.{Functor, Order, Show}
import com.evolutiongaming.catshelper.RandomIdOf
import com.evolutiongaming.skafka.Topic

final case class Key(id: String, topic: Topic) {

  override def toString = s"$topic:$id"
}

object Key {

  implicit val eqKey: Eq[Key] = Eq.fromUniversalEquals

  implicit val showKey: Show[Key] = Show.fromToString

  implicit val orderKey: Order[Key] = Order.whenEqual(Order.by { (a: Key) => a.topic }, Order.by { (a: Key) => a.id })

  implicit val orderingKey: Ordering[Key] = orderKey.toOrdering

  def random[F[_]: Functor: RandomIdOf](topic: Topic): F[Key] = {
    for {
      randomId <- RandomIdOf[F].apply
    } yield {
      Key(id = randomId.value, topic = topic)
    }
  }
}
