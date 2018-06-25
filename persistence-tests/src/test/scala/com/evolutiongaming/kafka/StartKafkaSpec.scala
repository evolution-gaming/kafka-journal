package com.evolutiongaming.kafka

import org.scalatest.FunSuite

class StartKafkaSpec extends FunSuite {

  test("start and stop kafka") {
    val shutdown = StartKafka()
    shutdown()
  }
}
