package com.evolutiongaming.kafka.journal

object PayloadBinaryFromStr {

  def apply(a: String): Payload.Binary = {
    val attempt = scodec.codecs.utf8.encode(a)
    val byteVector = attempt.require.toByteVector
    Payload.Binary(byteVector)
  }
}
