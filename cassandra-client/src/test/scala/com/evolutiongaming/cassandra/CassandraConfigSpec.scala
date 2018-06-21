package com.evolutiongaming.cassandra

import com.datastax.driver.core.ProtocolOptions.Compression
import com.datastax.driver.core.ProtocolVersion
import com.evolutiongaming.nel.Nel
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}

class CassandraConfigSpec extends FunSuite with Matchers {

  test("apply from empty config") {
    val config = ConfigFactory.empty()
    CassandraConfig(config) shouldEqual CassandraConfig.Default
  }

  test("apply from config") {
    val config = ConfigFactory.parseURL(getClass.getResource("cluster.conf"))
    val expected = CassandraConfig(
      name = "name",
      port = 1,
      contactPoints = Nel("127.0.0.1", "127.0.0.2"),
      protocolVersion = Some(ProtocolVersion.V5),
      pooling = PoolingConfig.Default,
      query = QueryConfig.Default,
      reconnection = ReconnectionConfig.Default,
      socket = SocketConfig.Default,
      authentication = Some(AuthenticationConfig("username", "password")),
      loadBalancing = Some(LoadBalancingConfig.Default),
      speculativeExecution = Some(SpeculativeExecutionConfig.Default),
      compression = Compression.LZ4,
      logQueries = true)
    CassandraConfig(config) shouldEqual expected
  }
}