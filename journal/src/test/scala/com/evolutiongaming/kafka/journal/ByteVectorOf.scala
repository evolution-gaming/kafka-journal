package com.evolutiongaming.kafka.journal

import scodec.bits.ByteVector

object ByteVectorOf {

  def apply(clazz: Class[_], path: String): ByteVector = {
    val is = Option(clazz.getResourceAsStream(path)) getOrElse {
      sys.error(s"file not found at $path")
    }
    val bytes = new Array[Byte](is.available())
    is.read(bytes)
    ByteVector.view(bytes)
  }
}
