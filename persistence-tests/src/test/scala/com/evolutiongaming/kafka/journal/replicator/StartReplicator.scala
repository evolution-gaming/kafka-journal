package com.evolutiongaming.kafka.journal.replicator

import akka.actor.ActorSystem

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

object StartReplicator {
  type Shutdown = () => Future[Unit]

  object Shutdown {
    val Empty: Shutdown = () => Future.unit
  }

  def apply(system: ActorSystem, timeout: FiniteDuration): Shutdown = {
    val replicator = Replicator(system).get(timeout)
    () => replicator.shutdown().future
  }
}
