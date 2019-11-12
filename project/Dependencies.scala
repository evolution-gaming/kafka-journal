import sbt._

object Dependencies {

  val scalatest            = "org.scalatest"          %% "scalatest"             % "3.0.8"
  val `scala-java8-compat` = "org.scala-lang.modules" %% "scala-java8-compat"    % "0.9.0"
  val prometheus           = "io.prometheus"           % "simpleclient"          % "0.6.0"
  val `cassandra-driver`   = "com.datastax.cassandra"  % "cassandra-driver-core" % "3.7.2"
  val `play-json`          = "com.typesafe.play"      %% "play-json"             % "2.7.4"
  val `kind-projector`     = "org.typelevel"           % "kind-projector"        % "0.10.3"
  val `executor-tools`     = "com.evolutiongaming"    %% "executor-tools"        % "1.0.2"
  val `config-tools`       = "com.evolutiongaming"    %% "config-tools"          % "1.0.3"
  val `akka-serialization` = "com.evolutiongaming"    %% "akka-serialization"    % "1.0.3"
  val nel                  = "com.evolutiongaming"    %% "nel"                   % "1.3.4"
  val `future-helper`      = "com.evolutiongaming"    %% "future-helper"         % "1.0.6"
  val `kafka-launcher`     = "com.evolutiongaming"    %% "kafka-launcher"        % "0.0.8"
  val `cassandra-launcher` = "com.evolutiongaming"    %% "cassandra-launcher"    % "0.0.3"
  val hostname             = "com.evolutiongaming"    %% "hostname"              % "0.1.2"
  val scassandra           = "com.evolutiongaming"    %% "scassandra"            % "2.0.4"
  val `cassandra-sync`     = "com.evolutiongaming"    %% "cassandra-sync"        % "1.0.5"
  val `cats-helper`        = "com.evolutiongaming"    %% "cats-helper"           % "1.1.0"
  val random               = "com.evolutiongaming"    %% "random"                % "0.0.6"
  val retry                = "com.evolutiongaming"    %% "retry"                 % "1.0.2"
  val sstream              = "com.evolutiongaming"    %% "sstream"               % "0.2.0"
  val skafka               = "com.evolutiongaming"    %% "skafka"                % "7.1.1"
  val scache               = "com.evolutiongaming"    %% "scache"                % "2.1.1"

  object Cats {
    private val version = "2.0.0"
    val core   = "org.typelevel" %% "cats-core"   % version
    val effect = "org.typelevel" %% "cats-effect" % "2.0.0"
  }

  object Logback {
    private val version = "1.2.3"
    val core    = "ch.qos.logback" % "logback-core"    % version
    val classic = "ch.qos.logback" % "logback-classic" % version
  }

  object Slf4j {
    private val version = "1.7.28"
    val api                = "org.slf4j" % "slf4j-api"        % version
    val `log4j-over-slf4j` = "org.slf4j" % "log4j-over-slf4j" % version
  }

  object Akka {
    private val version = "2.5.26"
    val actor             = "com.typesafe.akka" %% "akka-actor"           % version
    val testkit           = "com.typesafe.akka" %% "akka-testkit"         % version
    val stream            = "com.typesafe.akka" %% "akka-stream"          % version
    val persistence       = "com.typesafe.akka" %% "akka-persistence"     % version
    val `persistence-tck` = "com.typesafe.akka" %% "akka-persistence-tck" % version
    val slf4j             = "com.typesafe.akka" %% "akka-slf4j"           % version
  }
  
  object Kafka {
    private val version = "2.3.1"
    val kafka           = "org.apache.kafka" %% "kafka"         % version
    val `kafka-clients` = "org.apache.kafka" %  "kafka-clients" % version
  }

  object Scodec {
    val core = "org.scodec" %% "scodec-core" % "1.11.4"
    val bits = "org.scodec" %% "scodec-bits" % "1.1.12"
  }

  object Smetrics {
    private val version = "0.0.7"
    val smetrics   = "com.evolutiongaming" %% "smetrics"            % version
    val prometheus = "com.evolutiongaming" %% "smetrics-prometheus" % version
  }

  object Pureconfig {
    private val version = "0.12.1"
    val pureconfig = "com.github.pureconfig" %% "pureconfig"      % version
    val cats       = "com.github.pureconfig" %% "pureconfig-cats" % version
  }
}