import sbt._

object Dependencies {

  val scalatest            = "org.scalatest"          %% "scalatest"             % "3.0.7"
  val `scala-java8-compat` = "org.scala-lang.modules" %% "scala-java8-compat"    % "0.9.0"
  val prometheus           = "io.prometheus"           % "simpleclient"          % "0.6.0"
  val `cats-par`           = "io.chrisdavenport"      %% "cats-par"              % "0.2.1"
  val `cassandra-driver`   = "com.datastax.cassandra"  % "cassandra-driver-core" % "3.7.1"
  val `play-json`          = "com.typesafe.play"      %% "play-json"             % "2.7.3"
  val pureconfig           = "com.github.pureconfig"  %% "pureconfig"            % "0.11.0"
  val `executor-tools`     = "com.evolutiongaming"    %% "executor-tools"        % "1.0.1"
  val `config-tools`       = "com.evolutiongaming"    %% "config-tools"          % "1.0.3"
  val `akka-serialization` = "com.evolutiongaming"    %% "akka-serialization"    % "1.0.1"
  val nel                  = "com.evolutiongaming"    %% "nel"                   % "1.3.3"
  val `future-helper`      = "com.evolutiongaming"    %% "future-helper"         % "1.0.5"
  val `kafka-launcher`     = "com.evolutiongaming"    %% "kafka-launcher"        % "0.0.6"
  val `cassandra-launcher` = "com.evolutiongaming"    %% "cassandra-launcher"    % "0.0.2"
  val hostname             = "com.evolutiongaming"    %% "hostname"              % "0.1.1"
  val scassandra           = "com.evolutiongaming"    %% "scassandra"            % "0.0.11"
  val `cassandra-sync`     = "com.evolutiongaming"    %% "cassandra-sync"        % "0.0.7"
  val `cats-helper`        = "com.evolutiongaming"    %% "cats-helper"           % "0.0.12"
  val random               = "com.evolutiongaming"    %% "random"                % "0.0.2"
  val retry                = "com.evolutiongaming"    %% "retry"                 % "0.0.1"

  object Cats {
    private val version = "1.6.0"
    val core   = "org.typelevel" %% "cats-core"   % version
    val effect = "org.typelevel" %% "cats-effect" % "1.3.1"
  }

  object Logback {
    private val version = "1.2.3"
    val core    = "ch.qos.logback" % "logback-core"    % version
    val classic = "ch.qos.logback" % "logback-classic" % version
  }

  object Slf4j {
    private val version = "1.7.26"
    val api                = "org.slf4j" % "slf4j-api"        % version
    val `log4j-over-slf4j` = "org.slf4j" % "log4j-over-slf4j" % version
  }

  object Akka {
    private val version = "2.5.23"
    val actor             = "com.typesafe.akka" %% "akka-actor"           % version
    val testkit           = "com.typesafe.akka" %% "akka-testkit"         % version
    val stream            = "com.typesafe.akka" %% "akka-stream"          % version
    val persistence       = "com.typesafe.akka" %% "akka-persistence"     % version
    val `persistence-tck` = "com.typesafe.akka" %% "akka-persistence-tck" % version
    val slf4j             = "com.typesafe.akka" %% "akka-slf4j"           % version
  }
  
  object Kafka {
    private val version = "2.1.1"
    val kafka           = "org.apache.kafka" %% "kafka"         % version
    val `kafka-clients` = "org.apache.kafka" %  "kafka-clients" % version
  }

  object Skafka {
    private val version = "3.0.9"
    val skafka      = "com.evolutiongaming" %% "skafka"            % version
    val prometheus  = "com.evolutiongaming" %% "skafka-prometheus" % version
  }

  object Scodec {
    val core = "org.scodec" %% "scodec-core" % "1.11.3"
    val bits = "org.scodec" %% "scodec-bits" % "1.1.11"
  }
}