package com.evolutiongaming.kafka.journal

import cats.FlatMap
import cats.implicits._
import com.datastax.driver.core.{GettableByNameData, SettableData}
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.scassandra.{DecodeRow, EncodeRow}
import com.evolutiongaming.skafka.Topic

final case class Key(id: String, topic: Topic) {
  override def toString = s"$topic:$id"
}

object Key {

  implicit val EncodeRowKey: EncodeRow[Key] = new EncodeRow[Key] {

    def apply[B <: SettableData[B]](data: B, value: Key) = {
      data
        .encode("id", value.id)
        .encode("topic", value.topic)
    }
  }

  implicit val DecodeRowKey: DecodeRow[Key] = new DecodeRow[Key] {

    def apply(data: GettableByNameData) = {
      Key(
        id = data.decode[String]("id"),
        topic = data.decode[Topic]("topic"))
    }
  }


  def random[F[_] : FlatMap : RandomId](topic: Topic): F[Key] = {
    for {
      id <- RandomId[F].get
    } yield {
      Key(id = id, topic = topic)
    }
  }
}