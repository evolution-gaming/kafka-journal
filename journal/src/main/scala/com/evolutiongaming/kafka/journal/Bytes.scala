package com.evolutiongaming.kafka.journal

final case class Bytes(value: Array[Byte]) {

  override def toString: String = {
    val bytes = value.length
    s"$productPrefix($bytes)"
  }
}

object Bytes {
  val Empty: Bytes = Bytes(Array.empty)
}
