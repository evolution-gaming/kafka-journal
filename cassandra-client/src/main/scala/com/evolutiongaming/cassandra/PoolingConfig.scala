package com.evolutiongaming.cassandra

import com.datastax.driver.core.{HostDistance, PoolingOptions => PoolingOptionsJ}
import com.evolutiongaming.config.ConfigHelper._
import com.typesafe.config.Config

import scala.concurrent.duration._


case class PoolingConfig(
  local: PoolingConfig.HostConfig = PoolingConfig.HostConfig.Local,
  remote: PoolingConfig.HostConfig = PoolingConfig.HostConfig.Remote,
  poolTimeout: FiniteDuration = 5.seconds,
  idleTimeout: FiniteDuration = 2.minutes,
  maxQueueSize: Int = 256,
  heartbeatInterval: FiniteDuration = 30.seconds) {

  import PoolingConfig._

  def asJava: PoolingOptionsJ = {
    new PoolingOptionsJ()
      .set(local, HostDistance.LOCAL)
      .set(remote, HostDistance.REMOTE)
      .setPoolTimeoutMillis(poolTimeout.toMillis.toInt)
      .setIdleTimeoutSeconds(idleTimeout.toSeconds.toInt)
      .setMaxQueueSize(maxQueueSize)
      .setHeartbeatIntervalSeconds(heartbeatInterval.toSeconds.toInt)
  }
}

object PoolingConfig {

  val Default: PoolingConfig = PoolingConfig()

  def apply(config: Config): PoolingConfig = {

    def group(name: String, default: HostConfig) = {
      config.getOpt[Config](name).fold(default) { config => HostConfig(config, default) }
    }

    def get[T: FromConf](name: String) = config.getOpt[T](name)

    PoolingConfig(
      local = group("local", Default.local),
      remote = group("remote", Default.remote),
      poolTimeout = get[FiniteDuration]("pool-timeout") getOrElse Default.poolTimeout,
      idleTimeout = get[FiniteDuration]("idle-timeout") getOrElse Default.idleTimeout,
      maxQueueSize = get[Int]("max-queue-size") getOrElse Default.maxQueueSize,
      heartbeatInterval = get[FiniteDuration]("heartbeat-interval") getOrElse Default.heartbeatInterval)
  }


  case class HostConfig(
    newConnectionThreshold: Int,
    maxRequestsPerConnection: Int,
    connectionsPerHostCore: Int,
    connectionsPerHostMax: Int)

  object HostConfig {

    val Local: HostConfig = HostConfig(
      newConnectionThreshold = 800,
      maxRequestsPerConnection = 32768,
      connectionsPerHostCore = 1,
      connectionsPerHostMax = 4)

    val Remote: HostConfig = HostConfig(
      newConnectionThreshold = 200,
      maxRequestsPerConnection = 2000,
      connectionsPerHostCore = 1,
      connectionsPerHostMax = 4)

    def apply(config: Config, default: HostConfig): HostConfig = {
      HostConfig(
        newConnectionThreshold = config.getOpt[Int]("new-connection-threshold") getOrElse default.newConnectionThreshold,
        maxRequestsPerConnection = config.getOpt[Int]("max-requests-per-connection") getOrElse default.maxRequestsPerConnection,
        connectionsPerHostCore = config.getOpt[Int]("connections-per-host-core") getOrElse default.connectionsPerHostCore,
        connectionsPerHostMax = config.getOpt[Int]("connections-per-host-max") getOrElse default.connectionsPerHostMax)
    }
  }


  implicit class PoolingOptionsJOps(val self: PoolingOptionsJ) extends AnyVal {

    def set(hostConfig: HostConfig, distance: HostDistance): PoolingOptionsJ = {
      self
        .setNewConnectionThreshold(distance, hostConfig.newConnectionThreshold)
        .setMaxRequestsPerConnection(distance, hostConfig.maxRequestsPerConnection)
        .setConnectionsPerHost(distance, hostConfig.connectionsPerHostCore, hostConfig.connectionsPerHostMax)
    }
  }
}
