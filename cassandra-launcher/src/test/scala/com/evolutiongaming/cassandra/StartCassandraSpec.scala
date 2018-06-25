package com.evolutiongaming.cassandra

import org.scalatest.FunSuite

class StartCassandraSpec extends FunSuite {

  test("start and stop cassandra") {
    val shutdown = StartCassandra()
    shutdown()
  }
}
