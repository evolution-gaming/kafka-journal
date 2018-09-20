package com.evolutiongaming.kafka.journal

import java.nio.ByteBuffer

import com.datastax.driver.core.{BoundStatement, Row}
import com.evolutiongaming.cassandra.{Decode, Encode}
import com.evolutiongaming.skafka.{FromBytes, ToBytes, Topic}

sealed abstract case class Bytes(value: Array[Byte]) {

  override def toString: String = {
    val bytes = value.length
    s"$productPrefix($bytes)"
  }
}

object Bytes {

  val Empty: Bytes = new Bytes(Array.empty) {}

  implicit val ToBytesImpl: ToBytes[Bytes] = new ToBytes[Bytes] {
    def apply(value: Bytes, topic: Topic) = value.value
  }

  implicit val FromBytesImpl: FromBytes[Bytes] = new FromBytes[Bytes] {
    def apply(bytes: Array[Byte], topic: Topic): Bytes = Bytes(bytes)
  }

  implicit val EncodeImpl: Encode[Bytes] = new Encode[Bytes] {
    def apply(statement: BoundStatement, name: String, value: Bytes) = {
      val bytes = ByteBuffer.wrap(value.value)
      statement.setBytes(name, bytes)
    }
  }

  implicit val DecodeImpl: Decode[Bytes] = new Decode[Bytes] {
    def apply(row: Row, name: String) = {
      val bytes = row.getBytes(name)
      Bytes(bytes.array())
    }
  }

  def apply(bytes: Array[Byte]): Bytes = if (bytes.isEmpty) Empty else new Bytes(bytes) {}
}