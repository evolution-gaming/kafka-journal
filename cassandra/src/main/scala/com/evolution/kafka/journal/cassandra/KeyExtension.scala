package com.evolution.kafka.journal.cassandra

import com.datastax.oss.driver.api.core.data.{GettableByName, SettableByName}
import com.evolution.kafka.journal.Key
import com.evolution.scassandra.syntax.*
import com.evolution.scassandra.{DecodeRow, EncodeRow}
import com.evolutiongaming.skafka.Topic

object KeyExtension {
  implicit val encodeRowKey: EncodeRow[Key] = new EncodeRow[Key] {
    def apply[B <: SettableByName[B]](data: B, value: Key): B = {
      data
        .encode("id", value.id)
        .encode("topic", value.topic)
    }
  }

  implicit val decodeRowKey: DecodeRow[Key] = new DecodeRow[Key] {
    def apply(data: GettableByName): Key = {
      Key(id = data.decode[String]("id"), topic = data.decode[Topic]("topic"))
    }
  }
}
