package com.evolutiongaming.kafka.journal

import cats.effect.*
import cats.effect.implicits.*
import cats.syntax.all.*
import com.evolutiongaming.catshelper.*
import com.evolutiongaming.kafka.journal.IOSuite.*
import com.evolutiongaming.kafka.journal.TestJsonCodec.instance
import com.evolutiongaming.kafka.journal.replicator.{Replicator, ReplicatorConfig}
import com.evolutiongaming.kafka.journal.util.*
import com.evolutiongaming.scassandra.CassandraClusterOf
import com.evolutiongaming.smetrics.CollectorRegistry
import com.github.dockerjava.api.model.{ExposedPort, HostConfig, PortBinding, Ports}
import com.typesafe.config.ConfigFactory
import org.testcontainers.cassandra.CassandraContainer
import org.testcontainers.containers.GenericContainer
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

object IntegrationSuite {
  // Cassandra version: latest 4.x
  private val CassandraDockerImage = DockerImageName.parse("cassandra:4.1.8")
  // Kafka version: 4.0.0 is too new to be the main test version, 3.9.0 container doesn't start
  private val KafkaDockerImage = DockerImageName.parse("apache/kafka-native:3.8.1")

  def startF[F[_]: Async: ToFuture: LogOf: MeasureDuration: FromTry: ToTry: Fail](
    cassandraClusterOf: CassandraClusterOf[F],
  ): Resource[F, Unit] = {

    def cassandraContainer(log: Log[F]): Resource[F, Unit] =
      testContainer(log, exposeStaticPort = 9042)(new CassandraContainer(CassandraDockerImage))

    def kafkaContainer(log: Log[F]): Resource[F, Unit] =
      testContainer(log, exposeStaticPort = 9092)(new KafkaContainer(KafkaDockerImage))

    def testContainer[C <: GenericContainer[C]](
      log: Log[F],
      exposeStaticPort: Int,
    )(makeContainer: => C): Resource[F, Unit] = {
      Resource.make {
        Sync[F].blocking {
          val container = makeContainer
            .withCreateContainerCmdModifier { cmd =>
              cmd.withHostConfig(
                new HostConfig()
                  .withPortBindings(new PortBinding(Ports.Binding.bindPort(exposeStaticPort), new ExposedPort(exposeStaticPort))),
              )
              ()
            }
          container.start()
          container
        }
      } { container =>
        Sync[F]
          .blocking { container.stop() }
          .onError {
            case t =>
              log.error(s"failed to stop $container: ${t.getMessage}", t)
          }
      }.void
    }

    def replicator(log: Log[F]) = {
      implicit val kafkaConsumerOf = KafkaConsumerOf[F]()
      val config = for {
        config <- Sync[F].delay { ConfigFactory.load("replicator.conf") }
        config <- ReplicatorConfig.fromConfig[F](config)
      } yield config

      for {
        metrics  <- Replicator.Metrics.make[F](CollectorRegistry.empty[F], "clientId")
        config   <- config.toResource
        hostName <- HostName.of[F]().toResource
        result   <- Replicator.make[F](config, cassandraClusterOf, hostName, metrics.some)
        _        <- result.onError { case e => log.error(s"failed to release replicator with $e", e) }.background
      } yield {}
    }

    for {
      log <- LogOf[F].apply(IntegrationSuite.getClass).toResource
      _   <- cassandraContainer(log) both kafkaContainer(log) // start in parallel
      _   <- replicator(log)
    } yield {}
  }

  def startIO(cassandraClusterOf: CassandraClusterOf[IO]): Resource[IO, Unit] = {
    import cats.effect.unsafe.implicits.global

    val logOf = LogOf.slf4j[IO]
    for {
      logOf <- logOf.toResource
      result <- {
        implicit val logOf1 = logOf
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
