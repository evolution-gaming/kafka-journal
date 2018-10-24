package com.evolutiongaming.kafka.journal

import com.datastax.driver.core.{BoundStatement, Row}
import com.evolutiongaming.scassandra.CassandraHelper._
import com.evolutiongaming.scassandra.{DecodeRow, EncodeRow}
import com.evolutiongaming.skafka.Topic

final case class Key(id: Id, topic: Topic) {
  override def toString = s"$topic:$id"
}

object Key {

  implicit val EncodeImpl: EncodeRow[Key] = new EncodeRow[Key] {
    def apply(statement: BoundStatement, value: Key) = {
      statement
        .encode("id", value.id)
        .encode("topic", value.topic)
    }
  }

  implicit val DecodeImpl: DecodeRow[Key] = new DecodeRow[Key] {
    def apply(row: Row) = {
      Key(
        id = row.decode[Id]("id"),
        topic = row.decode[Topic]("topic"))
    }
  }
}