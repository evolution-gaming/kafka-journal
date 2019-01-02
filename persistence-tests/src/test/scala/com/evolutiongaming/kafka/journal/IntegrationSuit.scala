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

  def startF[F[_] : Concurrent : Timer : Par : FromFuture : ToFuture : ContextShift](system: ActorSystem): Resource[F, Unit] = {

    def cassandra(log: Log[F]) = Resource {
      for {
        cassandra <- Sync[F].delay { StartCassandra() }
      } yield {
        val release = Sync[F].delay { cassandra() }.onError { case e =>
          log.error(s"failed to release cassandra with $e", e)
        }
        (().pure[F], release)
      }
    }

    def kafka(log: Log[F]) = Resource {
      for {
        kafka <- Sync[F].delay { StartKafka() }
      } yield {
        val release = Sync[F].delay { kafka() }.onError { case e =>
          log.error(s"failed to release kafka with $e", e)
        }
        (().pure[F], release)
      }
    }

    for {
      log <- Resource.liftF(Log.of[F](IntegrationSuit.getClass))
      _   <- cassandra(log)
      _   <- kafka(log)
      _   <- Replicator.of[F](system)
    } yield {}
  }

  def startIO(system: ActorSystem): Resource[IO, Unit] = {
    implicit val executionContext = system.dispatcher
    implicit val contextShift = IO.contextShift(executionContext)
    implicit val fromFuture = FromFuture.lift[IO]
    implicit val timer = IO.timer(executionContext)
    startF[IO](system)
  }

  private lazy val started: Unit = {
    val config = ConfigFactory.load("replicator.conf")
    val system = ActorSystem("replicator", config)
    val future = startIO(system).allocated.unsafeToFuture()
    val (_, release) = Await.result(future, 1.minute)

    CoordinatedShutdown.get(system).addJvmShutdownHook {
      val future = release.unsafeToFuture()
      try {
        Await.result(future, 1.minute)
      } catch {
        case NonFatal(e) => e.printStackTrace()
      }
    }
  }

  def start(): Unit = started
}
