package com.evolutiongaming.kafka.journal

import akka.actor.{ActorSystem, CoordinatedShutdown}
import cats.effect._
import cats.implicits._
import com.evolutiongaming.cassandra.StartCassandra
import com.evolutiongaming.kafka.StartKafka
import com.evolutiongaming.kafka.journal.replicator.Replicator
import com.evolutiongaming.kafka.journal.util.{FromFuture, Par, ToFuture}
import com.typesafe.config.ConfigFactory

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.control.NonFatal

object IntegrationSuit {

  def startF[F[_] : Concurrent : Timer : Par : FromFuture : ToFuture : ContextShift](system: ActorSystem): F[F[Unit]] = {

    val cassandra = for {
      cassandra <- Sync[F].delay { StartCassandra() }
    } yield {
      Sync[F].delay { cassandra() }
    }

    val kafka = for {
      kafka <- Sync[F].delay { StartKafka() }
    } yield {
      Sync[F].delay { kafka() }
    }

    for {
      log    <- Log.of[F](IntegrationSuit.getClass)
      ck     <- Par[F].tupleN(cassandra, kafka)
      (c, k)  = ck
      r      <- Replicator.of[F](system)
    } yield {
      for {
        _  <- r.close.handleErrorWith { e => log.error(s"failed to shutdown replicator with $e", e) }
        c1  = c.handleErrorWith       { e => log.error(s"failed to shutdown cassandra with $e", e) }
        k1  = k.handleErrorWith       { e => log.error(s"failed to shutdown kafka with $e", e) }
        _  <- Par[F].tupleN(c1, k1)
      } yield {}
    }
  }

  def startIO(system: ActorSystem): IO[IO[Unit]] = {
    implicit val executionContext = system.dispatcher
    implicit val contextShift = IO.contextShift(executionContext)
    implicit val fromFuture = FromFuture.lift[IO]
    implicit val timer = IO.timer(executionContext)
    startF[IO](system)
  }

  private lazy val started: Unit = {
    val config = ConfigFactory.load("replicator.conf")
    val system = ActorSystem("replicator", config)
    val future = startIO(system).unsafeToFuture()
    val shutdown = Await.result(future, 1.minute)

    CoordinatedShutdown.get(system).addJvmShutdownHook {
      val future = shutdown.unsafeToFuture()
      try {
        Await.result(future, 1.minute)
      } catch {
        case NonFatal(e) => e.printStackTrace()
      }
    }
  }

  def start(): Unit = started
}
