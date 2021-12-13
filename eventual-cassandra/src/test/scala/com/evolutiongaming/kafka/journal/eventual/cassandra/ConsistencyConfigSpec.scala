package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.syntax.all._
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig.ConsistencyConfig
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import pureconfig.ConfigSource

class ConsistencyConfigSpec extends AnyFunSuite with Matchers {

  test("configReader") {
    ConfigSource
      .empty
      .load[ConsistencyConfig] shouldEqual ConsistencyConfig.default.asRight
  }

  /*test("Read.configReader") {
    ConfigSource
      .empty
      .load[ConsistencyConfig.Read] shouldEqual ConsistencyConfig.Read.default.asRight
  }*/

  /*test("Write.configReader") {
    ConfigSource
      .empty
      .load[ConsistencyConfig.Write] shouldEqual ConsistencyConfig.Write.default.asRight
  }*/
}
