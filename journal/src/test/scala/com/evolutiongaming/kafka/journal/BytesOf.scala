package com.evolutiongaming.kafka.journal

object BytesOf {
  def apply(clazz: Class[_], path: String): Bytes = {
    val is = Option(clazz.getResourceAsStream(path)) getOrElse {
      sys.error(s"file not found at $path")
    }
    val bytes = new Bytes(is.available())
    is.read(bytes)
    bytes
  }
}
