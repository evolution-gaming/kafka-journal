import sbt.*

object Dependencies {

  val scalatest                  = "org.scalatest"          %% "scalatest"                      % "3.2.19"
  val `scala-java8-compat`       = "org.scala-lang.modules" %% "scala-java8-compat"             % "1.0.2"
  val `kind-projector`           = "org.typelevel"           % "kind-projector"                 % "0.13.3"
  val `cassandra-driver`         = "com.datastax.cassandra"  % "cassandra-driver-core"          % "3.11.2"
  val `play-json`                = "com.typesafe.play"      %% "play-json"                      % "2.9.3"
  val `play-json-jsoniter`       = "com.evolution"          %% "play-json-jsoniter"             % "1.1.1"
  val `executor-tools`           = "com.evolutiongaming"    %% "executor-tools"                 % "1.0.3"
  val `akka-serialization`       = "com.evolutiongaming"    %% "akka-serialization"             % "1.1.0"
  val `kafka-launcher`           = "com.evolutiongaming"    %% "kafka-launcher"                 % "0.2.0"
  val `testcontainers-cassandra` = "com.dimafeng"           %% "testcontainers-scala-cassandra" % "0.43.0"
  val hostname                   = "com.evolutiongaming"    %% "hostname"                       % "0.2.0"
  val scassandra                 = "com.evolutiongaming"    %% "scassandra"                     % "5.3.0"
  val `cassandra-sync`           = "com.evolutiongaming"    %% "cassandra-sync"                 % "3.0.0"
  val `cats-helper`              = "com.evolutiongaming"    %% "cats-helper"                    % "3.11.3"
  val random                     = "com.evolutiongaming"    %% "random"                         % "1.0.0"
  val retry                      = "com.evolutiongaming"    %% "retry"                          % "3.0.1"
  val sstream                    = "com.evolutiongaming"    %% "sstream"                        % "1.0.1"
  val skafka                     = "com.evolutiongaming"    %% "skafka"                         % "17.1.3"
  val `akka-test-actor`          = "com.evolutiongaming"    %% "akka-test-actor"                % "0.1.0"
  val scache                     = "com.evolution"          %% "scache"                         % "5.1.3"
  val `resource-pool`            = "com.evolution"          %% "resource-pool"                  % "1.0.5"

  object Cats {
    val core   = "org.typelevel" %% "cats-core"   % "2.13.0"
    val effect = "org.typelevel" %% "cats-effect" % "3.5.7"
  }

  object Logback {
    private val version = "1.5.18"
    val core            = "ch.qos.logback" % "logback-core"    % version
    val classic         = "ch.qos.logback" % "logback-classic" % version
  }

  object Slf4j {
    private val version    = "2.0.17"
    val api                = "org.slf4j" % "slf4j-api"        % version
    val `log4j-over-slf4j` = "org.slf4j" % "log4j-over-slf4j" % version
  }

  object Akka {
    private val version   = "2.6.21"
    val actor             = "com.typesafe.akka" %% "akka-actor"           % version
    val testkit           = "com.typesafe.akka" %% "akka-testkit"         % version
    val stream            = "com.typesafe.akka" %% "akka-stream"          % version
    val persistence       = "com.typesafe.akka" %% "akka-persistence"     % version
    val `persistence-tck` = "com.typesafe.akka" %% "akka-persistence-tck" % version
    val slf4j             = "com.typesafe.akka" %% "akka-slf4j"           % version
  }

  object Kafka {
    private val version = "3.4.0"
    val kafka           = "org.apache.kafka" %% "kafka"         % version
    val `kafka-clients` = "org.apache.kafka"  % "kafka-clients" % version
  }

  object Scodec {
    val core = "org.scodec" %% "scodec-core" % "1.11.7"
    val bits = "org.scodec" %% "scodec-bits" % "1.1.20"
  }

  object Smetrics {
    private val version = "2.2.0"
    val smetrics        = "com.evolutiongaming" %% "smetrics"            % version
    val prometheus      = "com.evolutiongaming" %% "smetrics-prometheus" % version
  }

  object Pureconfig {
    private val version = "0.17.8"
    val pureconfig      = "com.github.pureconfig" %% "pureconfig"      % version
    val cats            = "com.github.pureconfig" %% "pureconfig-cats" % version
  }

  object Circe {
    private val version = "0.14.12"
    val core            = "io.circe" %% "circe-core"    % version
    val generic         = "io.circe" %% "circe-generic" % version
    val jawn            = "io.circe" %% "circe-jawn"    % version
  }
}
