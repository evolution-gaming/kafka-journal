package com.evolutiongaming.cassandra

import org.scalatest.FunSuite

class StartCassandraSpec extends FunSuite {

  ignore("start and stop cassandra") {
    val shutdown = StartCassandra()
    shutdown()
  }
}
