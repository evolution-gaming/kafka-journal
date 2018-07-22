package com.evolutiongaming.cassandra

import com.datastax.driver.core.{Cluster => ClusterJ}
import com.evolutiongaming.cassandra.CassandraHelper._
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.concurrent.FutureHelper._

import scala.concurrent.Future

trait Cluster {

  def connect(): Future[Session]

  def connect(keyspace: String): Future[Session]

  def close(): Future[Unit]

  def clusterName: String

  def newSession(): Session
}

object Cluster {

  def apply(cluster: ClusterJ): Cluster = {

    implicit val ec = CurrentThreadExecutionContext

    new Cluster {

      def connect() = cluster.connectAsync().asScala().map(Session(_))

      def connect(keyspace: String) = cluster.connectAsync(keyspace).asScala().map(Session(_))

      def close() = cluster.closeAsync().asScala().flatMap(_ => Future.unit)

      def clusterName = cluster.getClusterName

      def newSession() = Session(cluster.newSession())
    }
  }
}
