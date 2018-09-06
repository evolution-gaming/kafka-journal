package com.evolutiongaming.kafka.journal

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

  def apply(bytes: Array[Byte]): Bytes = if (bytes.isEmpty) Empty else new Bytes(bytes) {}
}