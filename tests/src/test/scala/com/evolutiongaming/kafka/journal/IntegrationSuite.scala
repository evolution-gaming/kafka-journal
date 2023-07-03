package com.evolutiongaming.kafka.journal

import cats.Parallel
import cats.effect._
import cats.effect.implicits._
import cats.syntax.all._
import com.dimafeng.testcontainers.CassandraContainer
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper.{FromTry, Log, LogOf, MeasureDuration, ToFuture, ToTry}
import com.evolutiongaming.kafka.StartKafka
import com.evolutiongaming.kafka.journal.replicator.{Replicator, ReplicatorConfig}
import com.evolutiongaming.kafka.journal.util._
import com.evolutiongaming.kafka.journal.IOSuite._
import com.evolutiongaming.scassandra.CassandraClusterOf
import com.evolutiongaming.smetrics.CollectorRegistry
import com.github.dockerjava.api.model.{ExposedPort, HostConfig, PortBinding, Ports}
import com.typesafe.config.ConfigFactory
import TestJsonCodec.instance

import scala.concurrent.ExecutionContext


object IntegrationSuite {

  def startF[F[_]: Concurrent: Timer: Parallel: ToFuture: ContextShift: LogOf: MeasureDuration: FromTry: ToTry: Fail](
    cassandraClusterOf: CassandraClusterOf[F]
  ): Resource[F, Unit] = {

    def cassandra(log: Log[F]) = Resource {
      // To ensure all default '127.0.0.1:9042' configurations work, we need to bind to the host's 9042 port manually
      val cassandra = CassandraContainer().configure(_.withCreateContainerCmdModifier(cmd => cmd.withHostConfig(
        new HostConfig().withPortBindings(new PortBinding(Ports.Binding.bindPort(9042), new ExposedPort(9042)))
      )))
      for {
        _ <- Sync[F].delay { cassandra.start() }
      } yield {
        val release = Sync[F].delay { cassandra.stop() }.onError { case e =>
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

    def replicator(log: Log[F], blocking: ExecutionContext) = {
      implicit val kafkaConsumerOf = KafkaConsumerOf[F](blocking)
      val config = for {
        config <- Sync[F].delay { ConfigFactory.load("replicator.conf") }
        config <- ReplicatorConfig.fromConfig[F](config)
      } yield config

      for {
        metrics  <- Replicator.Metrics.of[F](CollectorRegistry.empty[F], "clientId")
        config   <- config.toResource
        hostName <- HostName.of[F]().toResource
        result   <- Replicator.of[F](config, cassandraClusterOf, hostName, metrics.some)
        _        <- result.onError { case e => log.error(s"failed to release replicator with $e", e) }.background
      } yield {}
    }

    for {
      log      <- LogOf[F].apply(IntegrationSuite.getClass).toResource
      _        <- cassandra(log)
      _        <- kafka(log)
      blocking <- Executors.blocking[F]("kafka-journal-blocking")
      _        <- replicator(log, blocking)
    } yield {}
  }

  def startIO(cassandraClusterOf: CassandraClusterOf[IO]): Resource[IO, Unit] = {
    val logOf = LogOf.slf4j[IO]
    for {
      logOf  <- logOf.toResource
      result <- {
        implicit val logOf1 = logOf
        startF[IO](cassandraClusterOf)
      }
    } yield result
  }

  private lazy val started: Unit = {
    val (_, release) = startIO(CassandraSuite.cassandraClusterOf).allocated.unsafeRunSync()

    val _ = sys.addShutdownHook { release.unsafeRunSync() }
  }

  def start(): Unit = started
}
