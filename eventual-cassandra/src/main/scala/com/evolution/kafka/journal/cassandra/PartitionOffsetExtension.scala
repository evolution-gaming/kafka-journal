package com.evolution.kafka.journal.cassandra

import com.datastax.oss.driver.api.core.data.{GettableByName, SettableByName}
import com.evolution.kafka.journal.PartitionOffset
import com.evolution.kafka.journal.cassandra.SkafkaHelperExtension.*
import com.evolution.scassandra.syntax.*
import com.evolution.scassandra.{DecodeRow, EncodeRow}
import com.evolutiongaming.skafka.{Offset, Partition}

object PartitionOffsetExtension {
  implicit val encodeRowPartitionOffset: EncodeRow[PartitionOffset] = new EncodeRow[PartitionOffset] {

    def apply[B <: SettableByName[B]](data: B, value: PartitionOffset): B = {
      data
        .encode("partition", value.partition)
        .encode("offset", value.offset)
    }
  }

  implicit val decodeRowPartitionOffset: DecodeRow[PartitionOffset] = (data: GettableByName) => {
    PartitionOffset(partition = data.decode[Partition]("partition"), offset = data.decode[Offset]("offset"))
  }
}
