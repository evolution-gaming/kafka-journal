import sbt._

object Dependencies {

  lazy val ScalaTest = "org.scalatest" %% "scalatest" % "3.0.5" % Test

  lazy val ExecutorTools = "com.evolutiongaming" %% "executor-tools" % "1.0.0"

  lazy val ConfigTools = "com.evolutiongaming" %% "config-tools" % "1.0.2"

  lazy val Skafka = "com.evolutiongaming" %% "skafka-impl" % "0.3.2"

  lazy val PubSub = "com.evolutiongaming" %% "pubsub" % "2.0.4"

  lazy val PlayJson = "com.typesafe.play" %% "play-json" % "2.6.9"

  lazy val ScalaTools = "com.evolutiongaming" %% "scala-tools" % "2.1"

  lazy val Nel = "com.evolutiongaming" %% "nel" % "1.2"

  lazy val CommonsIo = "org.apache.commons" % "commons-io" % "1.3.2"

  object Logback {
    private val version = "1.2.3"
    lazy val Core = "ch.qos.logback" % "logback-core" % version
    lazy val Classic = "ch.qos.logback" % "logback-classic" % version
  }

  object Slf4j {
    private val version = "1.7.25"
    lazy val Api = "org.slf4j" % "slf4j-api" % version
    lazy val OverLog4j = "org.slf4j" % "log4j-over-slf4j" % version
  }

  object Cassandra {
    lazy val Driver = "com.datastax.cassandra" % "cassandra-driver-core" % "3.5.0"
    lazy val Server = "org.apache.cassandra" % "cassandra-all" % "3.11.2" exclude("commons-logging", "commons-logging")
  }

  object Kafka {
    private val version = "1.1.0"
    val Server = "org.apache.kafka" %% "kafka" % version
    val Clients = "org.apache.kafka" % "kafka-clients" % version
  }

  object Akka {
    private val version = "2.5.13"
    lazy val Persistence = "com.typesafe.akka" %% "akka-persistence" % version
    lazy val Tck = "com.typesafe.akka" %% "akka-persistence-tck" % version % Test
    lazy val Slf4j = "com.typesafe.akka" %% "akka-slf4j" % version % Test
  }
}