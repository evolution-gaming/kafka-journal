package com.evolutiongaming.kafka.journal

import akka.actor.{ActorSystem, CoordinatedShutdown}
import cats.effect._
import cats.implicits._
import com.evolutiongaming.cassandra.StartCassandra
import com.evolutiongaming.kafka.StartKafka
import com.evolutiongaming.kafka.journal.replicator.Replicator
import com.evolutiongaming.kafka.journal.util.{FromFuture, Par, ResourceOf, ToFuture}
import com.typesafe.config.ConfigFactory


object IntegrationSuit {

  def startF[F[_] : Concurrent : Timer : Par : FromFuture : ToFuture : ContextShift : LogOf](system: ActorSystem): Resource[F, Unit] = {

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

    def replicator(log: Log[F]) = {
      for {
        a  <- Replicator.of[F](system)
        a1  = a.onError { case e => log.error(s"failed to release kafka with $e", e) }
        _  <- ResourceOf(Concurrent[F].start(a1))
      } yield {}
    }

    for {
      log <- Resource.liftF(LogOf[F].apply(IntegrationSuit.getClass))
      _   <- cassandra(log)
      _   <- kafka(log)
      _   <- replicator(log)
    } yield {}
  }

  def startIO(system: ActorSystem): Resource[IO, Unit] = {
    implicit val executionContext = system.dispatcher
    implicit val contextShift = IO.contextShift(executionContext)
    implicit val fromFuture = FromFuture.lift[IO]
    implicit val timer = IO.timer(executionContext)
    implicit val logOf = LogOf[IO](system)
    startF[IO](system)
  }

  private lazy val started: Unit = {
    val config = ConfigFactory.load("replicator.conf")
    val system = ActorSystem("replicator", config)
    val (_, release) = startIO(system).allocated.unsafeRunSync()

    CoordinatedShutdown.get(system).addJvmShutdownHook {
      release.unsafeRunSync()
    }
  }

  def start(): Unit = started
}
