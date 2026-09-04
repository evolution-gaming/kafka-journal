package com.evolution.kafka.journal.it

import cats.effect.*
import cats.effect.implicits.*
import cats.implicits.*
import com.evolution.kafka.journal.HostName
import com.evolution.kafka.journal.IOSuite.*
import com.evolution.kafka.journal.replicator.{Replicator, ReplicatorConfig}
import com.evolutiongaming.catshelper.*
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.{Logger, LoggerFactory}
import org.testcontainers.cassandra.CassandraContainer
import org.testcontainers.containers.GenericContainer
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

import java.net.InetSocketAddress
import java.time.Instant
import scala.reflect.{ClassTag, classTag}
import scala.util.Try

/**
 * Services environment shared between Kafka-Journal integration tests.
 *
 * Obtain an instance using [[SharedItEnv.require()]]. If the call succeeds, it means that these
 * services have been started:
 *   - Kafka in testcontainers
 *   - Cassandra in testcontainers
 *   - Kafka-Journal replicator in the same JVM
 *
 * Kafka and Cassandra containers are bound to random available ports - use [[SharedItEnv]] instance
 * values and methods to configure Kafka-Journal components connectivity.
 *
 * It is expected that integration tests using this component are run in a forked JVM, as it relies
 * on JVM death to clean up resources. It is a similar pattern as this one described in the
 * testcontainers JUnit integration docs:
 * [[https://testcontainers.com/guides/testcontainers-container-lifecycle/#_using_singleton_containers]]
 *
 * @param cassandraContactPoint
 *   Cassandra contact point host string + port
 * @param kafkaBootstrapServers
 *   Kafka bootstrap server string
 * @param defaultConfig
 *   default [[JournalItConfig]] with shared services connectivity params applied
 */
private[it] final class SharedItEnv private (
  val cassandraContactPoint: InetSocketAddress,
  val kafkaBootstrapServers: String,
  val defaultConfig: JournalItConfig,
  private val configLoader: SharedItEnv.ConfigLoader,
) {

  /**
   * Helper method to load [[JournalItConfig]] with shared integration test services connectivity
   * settings applied.
   *
   * It expects the per-suite config overrides to be in a classpath resource file named
   * `<test suite class name>.conf`.
   *
   * @param suiteClass
   *   test suite class
   */
  def loadSuiteConfig(suiteClass: Class[?]): JournalItConfig = {
    JournalItConfig.parseUnsafe(configLoader.loadUnsafe(
      testOverride = ConfigFactory.parseResources(s"${ suiteClass.getSimpleName }.conf"),
    ))
  }

  /**
   * [[loadSuiteConfig(java.lang.Class)]] but with a type param
   *
   * @tparam S
   *   test suite type
   */
  def loadSuiteConfig[S: ClassTag]: JournalItConfig = loadSuiteConfig(classTag[S].runtimeClass)
}

private[it] object SharedItEnv {
  import IntegrationTestInstances.*

  private val CassandraDockerImage = DockerImageName.parse("cassandra:5.0.8")
  private val KafkaDockerImage = DockerImageName.parse("apache/kafka-native:4.3.1")

  private val defaultConfigResourceName = "application.conf"

  private val logIo: Log[IO] = logOf(classOf[SharedItEnv]).unsafeRunSync()
  private val log: Logger = LoggerFactory.getLogger(classOf[SharedItEnv])

  private case object Cassandra extends TestContainerDef[CassandraContainer](
    image = CassandraDockerImage,
    makeContainer = new CassandraContainer(_),
    getConnString = _.getContactPoint.toString,
  )
  private case object Kafka extends TestContainerDef[KafkaContainer](
    image = KafkaDockerImage,
    makeContainer = new KafkaContainer(_),
    getConnString = _.getBootstrapServers,
  )

  private lazy val memoizedStartEnvResult: Either[Throwable, SharedItEnv] = {
    Try {
      // see SharedItEnv doc on why the release IO is discarded here
      val (env, _) = runEnvResource.allocated.unsafeRunSync()
      env
    }.toEither
  }

  /**
   * Starts [[SharedItEnv]] services on first call.
   *
   * In case the required services couldn't start, subsequent calls are failed with an exception
   * indicating the original issue. This helps to get faster feedback in case of issues when
   * multiple tests suites are run.
   */
  def require(): SharedItEnv = {
    memoizedStartEnvResult match {
      case Left(err) =>
        throw new RuntimeException(
          "journal test environment failed to start, fail fast after first attempt",
          err,
        )
      case Right(env) =>
        env
    }
  }

  private def runEnvResource: ResourceIO[SharedItEnv] = logResourceStart(name = "SharedItEnv") {
    for {
      cassandraAndKafka <- Cassandra.resource both Kafka.resource // start in parallel
      (cassandra, kafka) = cassandraAndKafka
      connParams = SharedServicesConnectivityParams(
        cassandraContactPoint = cassandra.getContactPoint,
        kafkaBootstrapServers = kafka.getBootstrapServers,
      )
      configLoader <- makeConfigLoader(connParams).toResource
      defaultConfig <- IO { JournalItConfig.parseUnsafe(configLoader.loadUnsafe()) }.toResource
      _ <- replicatorResource(defaultConfig.replicator)
    } yield new SharedItEnv(
      cassandraContactPoint = connParams.cassandraContactPoint,
      kafkaBootstrapServers = connParams.kafkaBootstrapServers,
      defaultConfig = defaultConfig,
      configLoader = configLoader,
    )
  }

  private def replicatorResource(config: ReplicatorConfig): ResourceIO[Unit] = {
    logResourceStart(name = "replicator") {
      for {
        hostName <- HostName.of[IO]().toResource
        replicatorRunIo <- Replicator.make[IO](config, cassandraClusterOf, hostName, metrics = none)
        _ <- replicatorRunIo.onError { t =>
          logIo.error(s"[replicator] died prematurely: ${ t.getMessage }", t)
        }.background
      } yield ()
    }
  }

  private def logResourceStart[T](name: String)(resource: ResourceIO[T]): ResourceIO[T] = {
    for {
      startTime <- IO {
        val startTime = Instant.now()
        log.info(s"[$name] starting...")
        startTime
      }.toResource
      result <- resource
      _ <- IO {
        val duration = java.time.Duration.between(startTime, Instant.now())
        log.info(s"[$name] started in $duration")
      }.toResource
    } yield result
  }

  private abstract class TestContainerDef[C <: GenericContainer[C]](
    image: DockerImageName,
    makeContainer: DockerImageName => C,
    getConnString: C => String,
  ) { this: Product =>
    private def name: String = productPrefix

    private def logLifeCycleEvent(msg: String): Unit = {
      log.info(s"[Test-container $name] $msg")
    }

    val resource: ResourceIO[C] = {
      Resource.make(
        IO.blocking {
          val startTime = Instant.now()
          logLifeCycleEvent(s"starting using image $image...")
          val container = makeContainer(image)
          container.start()
          val duration = java.time.Duration.between(startTime, Instant.now())
          logLifeCycleEvent(s"started in $duration on ${ getConnString(container) }")
          container
        },
      ) { container =>
        IO.blocking {
          val startTime = Instant.now()
          logLifeCycleEvent("stopping...")
          container.stop()
          val duration = java.time.Duration.between(startTime, Instant.now())
          logLifeCycleEvent(s"stopped in $duration")
        }.onError {
          case t =>
            logIo.error(s"[Test-container $name] failed to stop: ${ t.getMessage }", t)
        }
      }
    }
  }

  private final case class SharedServicesConnectivityParams(
    cassandraContactPoint: InetSocketAddress,
    kafkaBootstrapServers: String,
  ) {
    val configOverride: Config = ConfigFactory.parseString(
      s"""
         |shared-test-services {
         |  cassandra {
         |    host = "${ cassandraContactPoint.getHostString }"
         |    port = ${ cassandraContactPoint.getPort }
         |  }
         |  kafkaBootstrapServers = "$kafkaBootstrapServers"
         |}
         |""".stripMargin,
    )
  }

  private def makeConfigLoader(connParams: SharedServicesConnectivityParams): IO[ConfigLoader] = {
    IO.blocking {
      new ConfigLoader(
        rawOverrideConfig = ConfigFactory.defaultOverrides(),
        rawDefaultConfig = ConfigFactory.parseResources(defaultConfigResourceName),
        rawReferenceConfig = ConfigFactory.defaultReferenceUnresolved(),
        connParams = connParams,
      )
    }
  }

  private final class ConfigLoader(
    rawOverrideConfig: Config,
    rawDefaultConfig: Config,
    rawReferenceConfig: Config,
    connParams: SharedServicesConnectivityParams,
  ) {

    /**
     * Same config loading logic as [[ConfigFactory.load()]] but testcontainers connectivity params
     * are inserted with the default overrides (system properties, env variables) before resolution
     * happens, so we can use connectivity params in the default test config (`application.conf`).
     */
    def loadUnsafe(testOverride: Config = ConfigFactory.empty()): Config = {
      rawOverrideConfig
        .withFallback(connParams.configOverride)
        .withFallback(testOverride)
        .withFallback(rawDefaultConfig)
        .withFallback(rawReferenceConfig)
        .resolve()
    }
  }
}
