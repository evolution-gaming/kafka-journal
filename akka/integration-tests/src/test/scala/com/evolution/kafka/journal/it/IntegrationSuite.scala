package com.evolution.kafka.journal.it

import cats.effect.*
import cats.effect.implicits.*
import cats.implicits.*
import com.evolution.kafka.journal.*
import com.evolution.kafka.journal.IOSuite.*
import com.evolution.kafka.journal.replicator.{Replicator, ReplicatorConfig}
import com.evolutiongaming.catshelper.*
import com.evolutiongaming.smetrics.CollectorRegistry
import com.github.dockerjava.api.model.{ExposedPort, HostConfig, PortBinding, Ports}
import com.typesafe.config.ConfigFactory
import org.testcontainers.cassandra.CassandraContainer
import org.testcontainers.containers.GenericContainer
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

import scala.annotation.tailrec
import scala.concurrent.duration.*

private[it] object IntegrationSuite {
  private val CassandraDockerImage = DockerImageName.parse("cassandra:5.0.8")
  private val KafkaDockerImage = DockerImageName.parse("apache/kafka-native:4.3.1")

  import IntegrationTestInstances.*

  // Whether the throwable (or any cause in its chain) is a "host port already taken" failure.
  private def portInUse(error: Throwable): Boolean = {
    def indicatesPortInUse(t: Throwable): Boolean = {
      val message = Option(t.getMessage).fold("")(_.toLowerCase)
      message.contains("already allocated") || message.contains("already in use")
    }
    @tailrec
    def loop(t: Throwable): Boolean =
      indicatesPortInUse(t) ||
        (Option(t.getCause) match {
          case Some(cause) if cause ne t => loop(cause)
          case _ => false
        })
    loop(error)
  }

  private def startContainer[C <: GenericContainer[C]](
    exposeStaticPort: Int,
  )(
    makeContainer: => C,
  ): IO[C] = {
    IO {
      makeContainer.withCreateContainerCmdModifier { cmd =>
        cmd.withHostConfig(
          new HostConfig()
            .withPortBindings(new PortBinding(
              Ports.Binding.bindPort(exposeStaticPort),
              new ExposedPort(exposeStaticPort),
            )),
        )
        ()
      }
    }
      .flatMap { container =>
        IO.blocking { container.start() }
          .as(container)
          // remove the partially-created container so a retry can rebind the port
          .onError { case _ => IO.blocking { container.stop() }.handleError(_ => ()) }
      }
  }

  // The containers bind fixed host ports, so only one test module can use them at a time.
  // Under sbt 2 the forked-test JVM closes its classloader (and the cats-effect runtime)
  // before shutdown hooks run, so the previous module's cleanup fails to stop its container
  // and it leaks the port. The out-of-process testcontainers Ryuk reaper frees it a few
  // seconds after that JVM exits, so retry startup while the port is still held.
  private def startContainerWithRetry[C <: GenericContainer[C]](
    log: Log[IO],
    exposeStaticPort: Int,
    attemptsLeft: Int,
  )(
    makeContainer: => C,
  ): IO[C] =
    startContainer(exposeStaticPort)(makeContainer).handleErrorWith { error =>
      if (attemptsLeft <= 1 || !portInUse(error)) error.raiseError[IO, C]
      else
        log.warn(s"failed to start container on port $exposeStaticPort, retrying: ${ error.getMessage }") *>
          IO.sleep(3.seconds) *>
          startContainerWithRetry(log, exposeStaticPort, attemptsLeft - 1)(makeContainer)
    }

  private def testContainer[C <: GenericContainer[C]](
    log: Log[IO],
    exposeStaticPort: Int,
  )(
    makeContainer: => C,
  ): Resource[IO, Unit] =
    Resource
      .make {
        startContainerWithRetry(log, exposeStaticPort, attemptsLeft = 20)(makeContainer) // ~1 minute total
      } { container =>
        IO.blocking { container.stop() }
          .onError { case t => log.error(s"failed to stop $container: ${ t.getMessage }", t) }
      }
      .void

  private def startAllResource: ResourceIO[Unit] = {

    def cassandraContainer(log: Log[IO]): ResourceIO[Unit] =
      testContainer(log, exposeStaticPort = 9042)(new CassandraContainer(CassandraDockerImage))

    def kafkaContainer(log: Log[IO]): ResourceIO[Unit] =
      testContainer(log, exposeStaticPort = 9092)(new KafkaContainer(KafkaDockerImage))

    def replicator(log: Log[IO]) = {
      val config = for {
        config <- IO.blocking { ConfigFactory.load("replicator.conf") }
        config <- ReplicatorConfig.fromConfig[IO](config)
      } yield config

      for {
        metrics <- Replicator.Metrics.make[IO](CollectorRegistry.empty[IO], "clientId")
        config <- config.toResource
        hostName <- HostName.of[IO]().toResource
        result <- Replicator.make[IO](config, cassandraClusterOf, hostName, metrics.some)
        _ <- result.onError { case e => log.error(s"failed to release replicator with $e", e) }.background
      } yield {}
    }

    for {
      log <- LogOf[IO].apply(IntegrationSuite.getClass).toResource
      _ <- cassandraContainer(log) both kafkaContainer(log) // start in parallel
      _ <- replicator(log)
    } yield {}
  }

  private lazy val started: Unit = {

    val (_, release) = startAllResource.allocated.unsafeRunSync()

    val _ = sys.addShutdownHook { release.unsafeRunSync() }
  }

  def start(): Unit = started
}
