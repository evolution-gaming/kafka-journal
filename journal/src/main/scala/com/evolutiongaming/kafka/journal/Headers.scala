package com.evolutiongaming.kafka.journal

object Headers {

  val Empty: Headers = Map.empty

  def apply(headers: (String, String)*): Headers = Map(headers: _ *)
}
