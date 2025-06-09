package com.evolutiongaming.kafka.journal

object Headers {

  val empty: Headers = Map.empty

  def apply(headers: (String, String)*): Headers = Map(headers*)
}
