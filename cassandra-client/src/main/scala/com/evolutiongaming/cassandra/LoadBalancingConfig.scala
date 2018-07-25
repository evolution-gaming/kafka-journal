package com.evolutiongaming.cassandra

import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, LoadBalancingPolicy, TokenAwarePolicy}
import com.evolutiongaming.config.ConfigHelper._
import com.typesafe.config.Config

/**
  * See [[https://docs.datastax.com/en/developer/java-driver/3.5/manual/load_balancing/]]
  */
final case class LoadBalancingConfig(
  localDc: String = "localDc",
  usedHostsPerRemoteDc: Int = 0,
  allowRemoteDcsForLocalConsistencyLevel: Boolean = false) {

  def asJava: LoadBalancingPolicy = {
    val policy = DCAwareRoundRobinPolicy.builder
      .withLocalDc(localDc)
      .withUsedHostsPerRemoteDc(usedHostsPerRemoteDc)
      .build()
    new TokenAwarePolicy(policy)
  }
}

object LoadBalancingConfig {

  val Default: LoadBalancingConfig = LoadBalancingConfig()

  def apply(config: Config): LoadBalancingConfig = {

    def get[T: FromConf](name: String) = config.getOpt[T](name)

    LoadBalancingConfig(
      localDc = get[String]("local-dc") getOrElse Default.localDc,
      usedHostsPerRemoteDc = get[Int]("used-hosts-per-remote-dc") getOrElse Default.usedHostsPerRemoteDc,
      allowRemoteDcsForLocalConsistencyLevel = get[Boolean]("allow-remote-dcs-for-local-consistency-level") getOrElse Default.allowRemoteDcsForLocalConsistencyLevel)
  }
}
