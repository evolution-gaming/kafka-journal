package com.evolutiongaming.cassandra

import com.evolutiongaming.kafka.journal.{ActorSpec, IntegrationSuit, Safe}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.concurrent.Await
import scala.concurrent.duration._

class CassandraSpec extends WordSpec with BeforeAndAfterAll with Matchers with ActorSpec {

  private val config = CassandraConfig.Default

  private val cluster = CreateCluster(config)

  private val timeout = 30.seconds

  private lazy val session = Await.result(cluster.connect(), timeout)

  override def beforeAll() = {
    super.beforeAll()
    IntegrationSuit.start()
  }

  override def afterAll() = {
    Safe {
      Await.result(cluster.close(), timeout)
    }
    super.afterAll()
  }

  "Cassandra" should {

    "clusterName" in {
      val config = CassandraConfig.Default
      val cluster = CreateCluster(config)
      cluster.clusterName.startsWith(config.name) shouldEqual true
    }

    "connect" in {
      session
    }

    "Session" should {

      "init" in {
        Await.result(session.init, timeout)
      }

      "closed" in {
        session.closed shouldEqual false
      }

      "State" should {

        "connectedHosts" in {
          session.state.connectedHosts.nonEmpty shouldEqual true
        }

        "openConnections" in {
          val state = session.state
          for {
            host <- state.connectedHosts
          } {
            state.openConnections(host) should be > 0
          }
        }

        "trashedConnections" in {
          val state = session.state
          for {
            host <- state.connectedHosts
          } {
            state.trashedConnections(host) shouldEqual 0
          }
        }

        "inFlightQueries" in {
          val state = session.state
          for {
            host <- state.connectedHosts
          } {
            state.inFlightQueries(host) shouldEqual 0
          }
        }
      }
    }
  }
}
