import sbt._

object Dependencies {

  val scalatest = "org.scalatest" %% "scalatest" % "3.0.5"

  val `executor-tools` = "com.evolutiongaming" %% "executor-tools" % "1.0.1"

  val `config-tools` = "com.evolutiongaming" %% "config-tools" % "1.0.3"

  val `akka-serialization` = "com.evolutiongaming" %% "akka-serialization" % "0.0.3"

  val async = "com.evolutiongaming" %% "async" % "0.0.2"

  val `play-json` = "com.typesafe.play" %% "play-json" % "2.6.10"

  val `scala-tools` = "com.evolutiongaming" %% "scala-tools" % "2.2"

  val nel = "com.evolutiongaming" %% "nel" % "1.3.2"

  val `future-helper` = "com.evolutiongaming" %% "future-helper" % "1.0.3"

  val serially = "com.evolutiongaming" %% "serially" % "1.0.4"

  val `safe-actor` = "com.evolutiongaming" %% "safe-actor" % "1.7"

  val `kafka-launcher` = "com.evolutiongaming" %% "kafka-launcher" % "0.0.2"

  val `cassandra-launcher` = "com.evolutiongaming" %% "cassandra-launcher" % "0.0.1"

  val `cassandra-driver` = "com.datastax.cassandra" % "cassandra-driver-core" % "3.6.0"

  val prometheus = "io.prometheus" % "simpleclient" % "0.5.0"

  object Logback {
    private val version = "1.2.3"
    val core    = "ch.qos.logback" % "logback-core" % version
    val classic = "ch.qos.logback" % "logback-classic" % version
  }

  object Slf4j {
    private val version = "1.7.25"
    val api                = "org.slf4j" % "slf4j-api" % version
    val `log4j-over-slf4j` = "org.slf4j" % "log4j-over-slf4j" % version
  }

  object Akka {
    private val version = "2.5.17"
    val actor       = "com.typesafe.akka" %% "akka-actor" % version
    val stream      = "com.typesafe.akka" %% "akka-stream" % version
    val persistence = "com.typesafe.akka" %% "akka-persistence" % version
    val tck         = "com.typesafe.akka" %% "akka-persistence-tck" % version
    val slf4j       = "com.typesafe.akka" %% "akka-slf4j" % version
  }

  object Skafka {
    private val version = "3.0.0"
    val skafka      = "com.evolutiongaming" %% "skafka" % version
    val prometheus  = "com.evolutiongaming" %% "skafka-prometheus" % version
  }
}