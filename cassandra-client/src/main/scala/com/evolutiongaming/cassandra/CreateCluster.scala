package com.evolutiongaming.cassandra

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger

import com.datastax.driver.core.{Cluster, QueryLogger}

import scala.collection.JavaConverters._

object CreateCluster {
  private val clusterId = new AtomicInteger(0)

  def apply(config: CassandraConfig): Cluster = {

    val port = config.port

    val contactPoints = config.contactPoints.map { contactPoint =>
      contactPoint.split(":").map(_.trim) match {
        case Array(host, port) => new InetSocketAddress(host, port.toInt)
        case Array(host)       => new InetSocketAddress(host, port)
        case _                 =>
          val msg = s"A contact point should be in form of [host:port] or [host], but is $contactPoint"
          throw new IllegalArgumentException(msg)
      }
    }

    val clusterName = s"${ config.name }-${ clusterId.getAndDecrement() }"

    val builder = Cluster.builder
      .addContactPointsWithPorts(contactPoints.toList.asJava)
      .withClusterName(clusterName)
      .withPoolingOptions(config.pooling.asJava)
      .withReconnectionPolicy(config.reconnection.asJava)
      .withQueryOptions(config.query.asJava)
      .withSocketOptions(config.socket.asJava)
      .withCompression(config.compression)
      .withPort(port)

    config.protocolVersion foreach { x => builder.withProtocolVersion(x) }
    config.authentication.foreach { x => builder.withCredentials(x.username, x.password) }
    config.loadBalancing.foreach { x => builder.withLoadBalancingPolicy(x.asJava) }
    config.speculativeExecution.foreach { x => builder.withSpeculativeExecutionPolicy(x.asJava) }

    val cluster = builder.build()

    if (config.logQueries) {
      val logger = QueryLogger.builder().build()
      cluster.register(logger)
    }

    cluster
  }
}
