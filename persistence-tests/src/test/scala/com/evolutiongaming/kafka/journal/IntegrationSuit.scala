package com.evolutiongaming.kafka.journal

import akka.actor.{ActorSystem, CoordinatedShutdown}
import com.evolutiongaming.cassandra.StartCassandra
import com.evolutiongaming.kafka.StartKafka
import com.evolutiongaming.kafka.journal.replicator.StartReplicator
import com.typesafe.config.ConfigFactory

import scala.concurrent.Await
import scala.concurrent.duration._

object IntegrationSuit {

  private lazy val started = {
    val shutdownCassandra = StartCassandra()
    val shutdownKafka = StartKafka()
    val timeout = 30.seconds
    val system = ActorSystem("replicator", ConfigFactory.load("replicator.conf"))
    val shutdownReplicator = StartReplicator(system, timeout)

    CoordinatedShutdown.get(system).addJvmShutdownHook {
      Safe {
        Await.result(shutdownReplicator(), timeout)
      }
      Safe {
        shutdownCassandra()
      }
      Safe {
        shutdownKafka()
      }
    }
  }

  def start(): Unit = started
}
