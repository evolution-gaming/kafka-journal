package com.evolutiongaming.kafka.journal

import cats.syntax.all._
import cats.kernel.Eq
import cats.{Functor, Order, Show}
import com.datastax.driver.core.{GettableByNameData, SettableData}
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.scassandra.{DecodeRow, EncodeRow}
import com.evolutiongaming.skafka.Topic

final case class Key(id: String, topic: Topic) {

  override def toString = s"$topic:$id"
}

object Key {

  implicit val eqKey: Eq[Key] = Eq.fromUniversalEquals

  implicit val showKey: Show[Key] = Show.fromToString

  implicit val orderKey: Order[Key] = Order.whenEqual(
    Order.by { a: Key => a.topic },
    Order.by { a: Key => a.id })

  implicit val orderingKey: Ordering[Key] = orderKey.toOrdering


  implicit val encodeRowKey: EncodeRow[Key] = new EncodeRow[Key] {

    def apply[B <: SettableData[B]](data: B, value: Key) = {
      data
        .encode("id", value.id)
        .encode("topic", value.topic)
    }
  }

  implicit val decodeRowKey: DecodeRow[Key] = new DecodeRow[Key] {

    def apply(data: GettableByNameData) = {
      Key(
        id = data.decode[String]("id"),
        topic = data.decode[Topic]("topic"))
    }
  }


  def random[F[_] : Functor : RandomIdOf](topic: Topic): F[Key] = {
    for {
      randomId <- RandomIdOf[F].apply
    } yield {
      Key(id = randomId.value, topic = topic)
    }
  }
}