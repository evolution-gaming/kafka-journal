import sbt._

object Dependencies {

  val scalatest = "org.scalatest" %% "scalatest" % "3.0.5"

  val `executor-tools` = "com.evolutiongaming" %% "executor-tools" % "1.0.1"

  val `config-tools` = "com.evolutiongaming" %% "config-tools" % "1.0.3"

  val `akka-serialization` = "com.evolutiongaming" %% "akka-serialization" % "0.0.3"

  val async = "com.evolutiongaming" %% "async" % "0.0.3"

  val `play-json` = "com.typesafe.play" %% "play-json" % "2.6.10"

  val `scala-tools` = "com.evolutiongaming" %% "scala-tools" % "2.2"

  val nel = "com.evolutiongaming" %% "nel" % "1.3.3"

  val `future-helper` = "com.evolutiongaming" %% "future-helper" % "1.0.3"

  val serially = "com.evolutiongaming" %% "serially" % "1.0.4"

  val `safe-actor` = "com.evolutiongaming" %% "safe-actor" % "2.0.0"

  val `kafka-launcher` = "com.evolutiongaming" %% "kafka-launcher" % "0.0.3"

  val `cassandra-launcher` = "com.evolutiongaming" %% "cassandra-launcher" % "0.0.1"

  val hostname = "com.evolutiongaming" %% "hostname" % "0.1.1"

  val scassandra = "com.evolutiongaming" %% "scassandra" % "0.0.1"

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
    private val version = "2.5.18"
    val actor             = "com.typesafe.akka" %% "akka-actor" % version
    val testkit           = "com.typesafe.akka" %% "akka-testkit" % version
    val stream            = "com.typesafe.akka" %% "akka-stream" % version
    val persistence       = "com.typesafe.akka" %% "akka-persistence" % version
    val `persistence-tck` = "com.typesafe.akka" %% "akka-persistence-tck" % version
    val slf4j             = "com.typesafe.akka" %% "akka-slf4j" % version
  }

  object Skafka {
    private val version = "3.0.2"
    val skafka      = "com.evolutiongaming" %% "skafka" % version
    val prometheus  = "com.evolutiongaming" %% "skafka-prometheus" % version
  }
}