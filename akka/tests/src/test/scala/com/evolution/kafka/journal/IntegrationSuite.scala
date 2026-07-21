package com.evolution.kafka.journal

import cats.effect.*
import cats.effect.implicits.*
import cats.syntax.all.*
import com.evolution.kafka.journal.IOSuite.*
import com.evolution.kafka.journal.TestJsonCodec.instance
import com.evolution.kafka.journal.replicator.{Replicator, ReplicatorConfig}
import com.evolution.kafka.journal.util.*
import com.evolutiongaming.catshelper.*
import com.evolutiongaming.scassandra.CassandraClusterOf
import com.evolutiongaming.smetrics.CollectorRegistry
import com.github.dockerjava.api.model.{ExposedPort, HostConfig, PortBinding, Ports}
import com.typesafe.config.ConfigFactory
import org.testcontainers.cassandra.CassandraContainer
import org.testcontainers.containers.GenericContainer
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

import scala.concurrent.duration.*

object IntegrationSuite {
  // Cassandra version: latest 4.x
  private val CassandraDockerImage = DockerImageName.parse("cassandra:4.1.8")
  // Kafka version: 4.0.0 is too new to be the main test version, 3.9.0 container doesn't start
  private val KafkaDockerImage = DockerImageName.parse("apache/kafka-native:3.8.1")

  // Whether the throwable (or any cause in its chain) is a "host port already taken" failure.
  private def portInUse(error: Throwable): Boolean = {
    def indicatesPortInUse(t: Throwable): Boolean = {
      val message = Option(t.getMessage).fold("")(_.toLowerCase)
      message.contains("already allocated") || message.contains("already in use")
    }
    def loop(t: Throwable): Boolean =
      indicatesPortInUse(t) ||
        (Option(t.getCause) match {
          case Some(cause) if cause ne t => loop(cause)
          case _ => false
        })
    loop(error)
  }

  private def startContainer[F[_]: Sync, C <: GenericContainer[C]](
    exposeStaticPort: Int,
  )(
    makeContainer: => C,
  ): F[C] =
    Sync[F]
      .blocking {
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
        Sync[F]
          .blocking { container.start() }
          .as(container)
          // remove the partially-created container so a retry can rebind the port
          .onError { case _ => Sync[F].blocking { container.stop() }.handleError(_ => ()) }
      }

  // The containers bind fixed host ports, so only one test module can use them at a time.
  // Under sbt 2 the forked-test JVM closes its classloader (and the cats-effect runtime)
  // before shutdown hooks run, so the previous module's cleanup fails to stop its container
  // and it leaks the port. The out-of-process testcontainers Ryuk reaper frees it a few
  // seconds after that JVM exits, so retry startup while the port is still held.
  private def startContainerWithRetry[F[_]: Async, C <: GenericContainer[C]](
    log: Log[F],
    exposeStaticPort: Int,
    attemptsLeft: Int,
  )(
    makeContainer: => C,
  ): F[C] =
    startContainer(exposeStaticPort)(makeContainer).handleErrorWith { error =>
      if (attemptsLeft <= 1 || !portInUse(error)) error.raiseError[F, C]
      else
        log.warn(s"failed to start container on port $exposeStaticPort, retrying: ${ error.getMessage }") *>
          Temporal[F].sleep(3.seconds) *>
          startContainerWithRetry(log, exposeStaticPort, attemptsLeft - 1)(makeContainer)
    }

  private def testContainer[F[_]: Async, C <: GenericContainer[C]](
    log: Log[F],
    exposeStaticPort: Int,
  )(
    makeContainer: => C,
  ): Resource[F, Unit] =
    Resource
      .make {
        startContainerWithRetry(log, exposeStaticPort, attemptsLeft = 20)(makeContainer) // ~1 minute total
      } { container =>
        Sync[F]
          .blocking { container.stop() }
          .onError { case t => log.error(s"failed to stop $container: ${ t.getMessage }", t) }
      }
      .void

  def startF[F[_]: Async: LogOf: MeasureDuration: FromTry: ToTry: Fail](
    cassandraClusterOf: CassandraClusterOf[F],
  ): Resource[F, Unit] = {

    def cassandraContainer(log: Log[F]): Resource[F, Unit] =
      testContainer(log, exposeStaticPort = 9042)(new CassandraContainer(CassandraDockerImage))

    def kafkaContainer(log: Log[F]): Resource[F, Unit] =
      testContainer(log, exposeStaticPort = 9092)(new KafkaContainer(KafkaDockerImage))

    def replicator(log: Log[F]) = {
      implicit val kafkaConsumerOf: KafkaConsumerOf[F] = KafkaConsumerOf[F]()
      val config = for {
        config <- Sync[F].delay { ConfigFactory.load("replicator.conf") }
        config <- ReplicatorConfig.fromConfig[F](config)
      } yield config

      for {
        metrics <- Replicator.Metrics.make[F](CollectorRegistry.empty[F], "clientId")
        config <- config.toResource
        hostName <- HostName.of[F]().toResource
        result <- Replicator.make[F](config, cassandraClusterOf, hostName, metrics.some)
        _ <- result.onError { case e => log.error(s"failed to release replicator with $e", e) }.background
      } yield {}
    }

    for {
      log <- LogOf[F].apply(IntegrationSuite.getClass).toResource
      _ <- cassandraContainer(log) both kafkaContainer(log) // start in parallel
      _ <- replicator(log)
    } yield {}
  }

  def startIO(cassandraClusterOf: CassandraClusterOf[IO]): Resource[IO, Unit] = {
    import cats.effect.unsafe.implicits.global

    val logOf = LogOf.slf4j[IO]
    for {
      logOf <- logOf.toResource
      result <- {
        implicit val logOf1: LogOf[IO] = logOf
        startF[IO](cassandraClusterOf)
      }
    } yield result
  }

  private lazy val started: Unit = {
    import cats.effect.unsafe.implicits.global

    val (_, release) = startIO(CassandraSuite.cassandraClusterOf).allocated.unsafeRunSync()

    val _ = sys.addShutdownHook { release.unsafeRunSync() }
  }

  def start(): Unit = started
}
