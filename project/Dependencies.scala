import sbt.*

object Dependencies {
  val ScalaTest = "org.scalatest" %% "scalatest" % "3.2.19"
  val ScalaJava8Compat = "org.scala-lang.modules" %% "scala-java8-compat" % "1.0.2"
  val KindProjector = "org.typelevel" % "kind-projector" % "0.13.3"
  val CassandraDriver = "com.datastax.cassandra" % "cassandra-driver-core" % "3.11.2"
  val KafkaClients = "org.apache.kafka" % "kafka-clients" % "3.4.0"
  val PlayJson = "com.typesafe.play" %% "play-json" % "2.10.7"

  private val PekkoExtensionVersion = "1.1.0"
  val PlayJsonJsoniter = "com.evolution" %% "play-json-jsoniter" % "1.2.3"
  val ExecutorTools = "com.evolutiongaming" %% "executor-tools" % "1.0.5"
  val AkkaSerialization = "com.evolutiongaming" %% "akka-serialization" % "1.1.0"
  val PekkoSerialization = "com.evolution" %% "pekko-extension-serialization" % PekkoExtensionVersion
  val Hostname = "com.evolutiongaming" %% "hostname" % "1.0.0"
  val SCassandra = "com.evolutiongaming" %% "scassandra" % "5.3.0"
  val CassandraSync = "com.evolutiongaming" %% "cassandra-sync" % "3.1.1"
  val CatsHelper = "com.evolutiongaming" %% "cats-helper" % "3.12.2"
  val Random = "com.evolution" %% "random" % "1.0.5"
  val Retry = "com.evolutiongaming" %% "retry" % "3.1.0"
  val SStream = "com.evolutiongaming" %% "sstream" % "1.1.0"
  val SKafka = "com.evolutiongaming" %% "skafka" % "17.2.2"
  val AkkaTestActor = "com.evolutiongaming" %% "akka-test-actor" % "0.3.0"
  val PekkoTestActor = "com.evolution" %% "pekko-extension-test-actor" % PekkoExtensionVersion
  val SCache = "com.evolution" %% "scache" % "5.1.4"
  val ResourcePool = "com.evolution" %% "resource-pool" % "1.0.6"

  object Cats {
    val Core = "org.typelevel" %% "cats-core" % "2.13.0"
    val Effect = "org.typelevel" %% "cats-effect" % "3.5.7"
  }

  object Logback {
    private val version = "1.5.19"
    val Core = "ch.qos.logback" % "logback-core" % version
    val Classic = "ch.qos.logback" % "logback-classic" % version
  }

  object Slf4j {
    private val version = "2.0.17"
    val Api = "org.slf4j" % "slf4j-api" % version
    val Log4jOverSlf4j = "org.slf4j" % "log4j-over-slf4j" % version
  }

  object Akka {
    private val version = "2.6.21"
    val Actor = "com.typesafe.akka" %% "akka-actor" % version
    val Testkit = "com.typesafe.akka" %% "akka-testkit" % version
    val Stream = "com.typesafe.akka" %% "akka-stream" % version
    val Persistence = "com.typesafe.akka" %% "akka-persistence" % version
    val PersistenceTck = "com.typesafe.akka" %% "akka-persistence-tck" % version
    val Slf4j = "com.typesafe.akka" %% "akka-slf4j" % version
  }

  object Pekko {
    private val version = "1.2.0"
    val Actor = "org.apache.pekko" %% "pekko-actor" % version
    val Testkit = "org.apache.pekko" %% "pekko-testkit" % version
    val Stream = "org.apache.pekko" %% "pekko-stream" % version
    val Persistence = "org.apache.pekko" %% "pekko-persistence" % version
    val PersistenceTck = "org.apache.pekko" %% "pekko-persistence-tck" % version
    val Slf4j = "org.apache.pekko" %% "pekko-slf4j" % version
  }

  object Scodec {
    val Bits = "org.scodec" %% "scodec-bits" % "1.2.4"
    object Scala2 {
      val Core = "org.scodec" %% "scodec-core" % "1.11.11" // the last scodec-core version built for 2.13
    }

    object Scala3 {
      val Core = "org.scodec" %% "scodec-core" % "2.3.3"
    }
  }

  object Smetrics {
    private val version = "2.3.2"
    val SMetrics = "com.evolutiongaming" %% "smetrics" % version
    val Prometheus = "com.evolutiongaming" %% "smetrics-prometheus" % version
  }

  object Pureconfig {
    private val version = "0.17.8"
    val Core = "com.github.pureconfig" %% "pureconfig-core" % version
    val Cats = "com.github.pureconfig" %% "pureconfig-cats" % version
    object Scala2 {
      val Generic = "com.github.pureconfig" %% "pureconfig-generic" % version
    }
    object Scala3 {
      val Generic = "com.github.pureconfig" %% "pureconfig-generic-scala3" % version
    }
  }

  object Circe {
    private val version = "0.14.14"
    val Core = "io.circe" %% "circe-core" % version
    val Generic = "io.circe" %% "circe-generic" % version
    val Jawn = "io.circe" %% "circe-jawn" % version
  }

  object TestContainers {
    private val version = "1.21.3"
    val Kafka = "org.testcontainers" % "kafka" % version
    val Cassandra = "org.testcontainers" % "cassandra" % version
  }
}
