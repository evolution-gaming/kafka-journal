package com.evolutiongaming.cassandra

import com.datastax.driver.core.SocketOptions
import com.evolutiongaming.config.ConfigHelper._
import com.typesafe.config.Config

import scala.concurrent.duration._

/**
  * See [[https://docs.datastax.com/en/developer/java-driver/3.5/manual/socket_options/]]
  */
case class SocketConfig(
  connectTimeout: FiniteDuration = 5.seconds,
  readTimeout: FiniteDuration = 12.seconds,
  keepAlive: Option[Boolean] = None,
  reuseAddress: Option[Boolean] = None,
  soLinger: Option[Int] = None,
  tcpNoDelay: Option[Boolean] = Some(true),
  receiveBufferSize: Option[Int] = None,
  sendBufferSize: Option[Int] = None) {

  def asJava: SocketOptions = {
    val socketOptions = new SocketOptions()
      .setConnectTimeoutMillis(connectTimeout.toMillis.toInt)
      .setReadTimeoutMillis(readTimeout.toMillis.toInt)

    keepAlive.foreach(socketOptions.setKeepAlive)
    reuseAddress.foreach(socketOptions.setReuseAddress)
    soLinger.foreach(socketOptions.setSoLinger)
    tcpNoDelay.foreach(socketOptions.setTcpNoDelay)
    receiveBufferSize.foreach(socketOptions.setReceiveBufferSize)
    sendBufferSize.foreach(socketOptions.setSendBufferSize)

    socketOptions
  }
}

object SocketConfig {

  val Default: SocketConfig = SocketConfig()

  def apply(config: Config): SocketConfig = {

    def get[T: FromConf](name: String) = config.getOpt[T](name)

    SocketConfig(
      connectTimeout = get[FiniteDuration]("connect-timeout") getOrElse Default.connectTimeout,
      readTimeout = get[FiniteDuration]("read-timeout") getOrElse Default.readTimeout,
      keepAlive = get[Boolean]("keep-alive") orElse Default.keepAlive,
      reuseAddress = get[Boolean]("reuse-address") orElse Default.reuseAddress,
      soLinger = get[Int]("so-linger") orElse Default.soLinger,
      tcpNoDelay = get[Boolean]("tcp-no-delay") orElse Default.tcpNoDelay,
      receiveBufferSize = get[Int]("receive-buffer-size") orElse Default.receiveBufferSize,
      sendBufferSize = get[Int]("send-buffer-size") orElse Default.sendBufferSize)
  }
}
