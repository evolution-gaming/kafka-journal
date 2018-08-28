package com.evolutiongaming.kafka.journal

sealed abstract case class Bytes(value: Array[Byte]) {

  override def toString: String = {
    val bytes = value.length
    s"$productPrefix($bytes)"
  }
}

object Bytes {
  val Empty: Bytes = new Bytes(Array.empty) {}

  def apply(bytes: Array[Byte]): Bytes = if (bytes.isEmpty) Empty else new Bytes(bytes) {}
}
