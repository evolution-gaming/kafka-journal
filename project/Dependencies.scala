import sbt._

object Dependencies {

  lazy val ScalaTest = "org.scalatest" %% "scalatest" % "3.0.5" % Test

  lazy val ExecutorTools = "com.evolutiongaming" %% "executor-tools" % "1.0.0"

  lazy val ConfigTools = "com.evolutiongaming" %% "config-tools" % "1.0.1"

  lazy val Skafka = "com.evolutiongaming" %% "skafka-impl" % "0.3.2"

  lazy val Logback = "ch.qos.logback" % "logback-classic" % "1.2.3" % Test

  lazy val Cassandra = "com.datastax.cassandra" % "cassandra-driver-core" % "3.3.1"

  lazy val PubSub = "com.evolutiongaming" %% "pubsub" % "2.0.4"

  lazy val PlayJson = "com.typesafe.play" %% "play-json" % "2.6.9"

  lazy val ScalaTools = "com.evolutiongaming" %% "scala-tools" % "2.1"

  object Kafka {
    private val version = "1.1.0"
    val Server = "org.apache.kafka" %% "kafka" % version % Test
    val Clients = "org.apache.kafka" % "kafka-clients" % version
  }

  object Akka {
    private val version = "2.5.12"
    lazy val Persistence = "com.typesafe.akka" %% "akka-persistence" % version
    lazy val Tck = "com.typesafe.akka" %% "akka-persistence-tck" % version % Test
    lazy val Slf4j = "com.typesafe.akka" %% "akka-slf4j" % version % Test
  }
}
