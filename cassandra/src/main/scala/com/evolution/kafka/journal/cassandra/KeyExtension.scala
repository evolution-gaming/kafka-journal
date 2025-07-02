package com.evolution.kafka.journal.cassandra

import com.datastax.driver.core.{GettableByNameData, SettableData}
import com.evolution.kafka.journal.Key
import com.evolutiongaming.scassandra.syntax.*
import com.evolutiongaming.scassandra.{DecodeRow, EncodeRow}
import com.evolutiongaming.skafka.Topic

object KeyExtension {
  implicit val encodeRowKey: EncodeRow[Key] = new EncodeRow[Key] {
    def apply[B <: SettableData[B]](data: B, value: Key): B = {
      data
        .encode("id", value.id)
        .encode("topic", value.topic)
    }
  }

  implicit val decodeRowKey: DecodeRow[Key] = new DecodeRow[Key] {
    def apply(data: GettableByNameData): Key = {
      Key(id = data.decode[String]("id"), topic = data.decode[Topic]("topic"))
    }
  }
}
