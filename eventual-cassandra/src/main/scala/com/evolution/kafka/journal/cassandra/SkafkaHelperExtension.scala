package com.evolution.kafka.journal.cassandra

import cats.syntax.all.*
import com.datastax.driver.core.{GettableByNameData, SettableData}
import com.evolutiongaming.scassandra.syntax.*
import com.evolutiongaming.scassandra.{DecodeByName, DecodeRow, EncodeByName, EncodeRow}
import com.evolutiongaming.skafka.{Offset, Partition}

import scala.util.Try

// TODO move to skafka & scassandra
object SkafkaHelperExtension {
  implicit val encodeByNamePartition: EncodeByName[Partition] = EncodeByName[Int].contramap { (a: Partition) =>
    a.value
  }

  implicit val decodeByNamePartition: DecodeByName[Partition] = DecodeByName[Int].map { a => Partition.of[Try](a).get }

  implicit val encodeByNameOffset: EncodeByName[Offset] = EncodeByName[Long].contramap { (a: Offset) => a.value }

  implicit val decodeByNameOffset: DecodeByName[Offset] = DecodeByName[Long].map { a => Offset.of[Try](a).get }

  implicit val decodeRowPartition: DecodeRow[Partition] = (data: GettableByNameData) => {
    data.decode[Partition]("partition")
  }

  implicit val encodeRowPartition: EncodeRow[Partition] = new EncodeRow[Partition] {

    def apply[B <: SettableData[B]](data: B, partition: Partition): B = {
      data.encode("partition", partition.value)
    }
  }

  implicit val decodeRowOffset: DecodeRow[Offset] = (data: GettableByNameData) => {
    data.decode[Offset]("offset")
  }

  implicit val encodeRowOffset: EncodeRow[Offset] = new EncodeRow[Offset] {

    def apply[B <: SettableData[B]](data: B, offset: Offset): B = {
      data.encode("offset", offset.value)
    }
  }
}
