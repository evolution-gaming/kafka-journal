package com.evolutiongaming.cassandra

import com.datastax.driver.core.policies.{ExponentialReconnectionPolicy, ReconnectionPolicy}
import com.evolutiongaming.config.ConfigHelper._
import com.typesafe.config.Config

import scala.concurrent.duration._

/**
  * See [[https://docs.datastax.com/en/developer/java-driver/3.5/manual/reconnection/]]
  */
case class ReconnectionConfig(
  minDelay: FiniteDuration = 1.second,
  maxDelay: FiniteDuration = 10.minutes) {

  def asJava: ReconnectionPolicy = {
    new ExponentialReconnectionPolicy(minDelay.toMillis, maxDelay.toMillis)
  }
}

object ReconnectionConfig {

  val Default: ReconnectionConfig = ReconnectionConfig()

  def apply(config: Config): ReconnectionConfig = {

    def get[T: FromConf](name: String) = config.getOpt[T](name)

    ReconnectionConfig(
      minDelay = get[FiniteDuration]("min-delay") getOrElse Default.minDelay,
      maxDelay = get[FiniteDuration]("max-delay") getOrElse Default.maxDelay)
  }
}
