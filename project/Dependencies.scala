import sbt._

object Dependencies {

  val scalatest            = "org.scalatest"          %% "scalatest"             % "3.0.8"
  val `scala-java8-compat` = "org.scala-lang.modules" %% "scala-java8-compat"    % "0.9.1"
  val prometheus           = "io.prometheus"           % "simpleclient"          % "0.6.0"
  val `cassandra-driver`   = "com.datastax.cassandra"  % "cassandra-driver-core" % "3.7.2"
  val `play-json`          = "com.typesafe.play"      %% "play-json"             % "2.7.4"
  val `kind-projector`     = "org.typelevel"           % "kind-projector"        % "0.11.0"
  val `play-json-jsoniter` = "com.evolutiongaming"    %% "play-json-jsoniter"    % "0.6.1"
  val `executor-tools`     = "com.evolutiongaming"    %% "executor-tools"        % "1.0.2"
  val `config-tools`       = "com.evolutiongaming"    %% "config-tools"          % "1.0.3"
  val `akka-serialization` = "com.evolutiongaming"    %% "akka-serialization"    % "1.0.3"
  val `future-helper`      = "com.evolutiongaming"    %% "future-helper"         % "1.0.6"
  val `kafka-launcher`     = "com.evolutiongaming"    %% "kafka-launcher"        % "0.0.9"
  val `cassandra-launcher` = "com.evolutiongaming"    %% "cassandra-launcher"    % "0.0.3"
  val hostname             = "com.evolutiongaming"    %% "hostname"              % "0.1.2"
  val scassandra           = "com.evolutiongaming"    %% "scassandra"            % "3.0.0"
  val `cassandra-sync`     = "com.evolutiongaming"    %% "cassandra-sync"        % "1.0.5"
  val `cats-helper`        = "com.evolutiongaming"    %% "cats-helper"           % "1.7.1"
  val random               = "com.evolutiongaming"    %% "random"                % "0.0.6"
  val retry                = "com.evolutiongaming"    %% "retry"                 % "1.0.2"
  val sstream              = "com.evolutiongaming"    %% "sstream"               % "0.2.0"
  val skafka               = "com.evolutiongaming"    %% "skafka"                % "9.0.2"
  val scache               = "com.evolutiongaming"    %% "scache"                % "2.2.0"
  val `akka-test-actor`    = "com.evolutiongaming"    %% "akka-test-actor"       % "0.0.2"

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
    private val version = "1.7.30"
    val api                = "org.slf4j" % "slf4j-api"        % version
    val `log4j-over-slf4j` = "org.slf4j" % "log4j-over-slf4j" % version
  }

  object Akka {
    private val version = "2.5.29"
    val actor             = "com.typesafe.akka" %% "akka-actor"           % version
    val testkit           = "com.typesafe.akka" %% "akka-testkit"         % version
    val stream            = "com.typesafe.akka" %% "akka-stream"          % version
    val persistence       = "com.typesafe.akka" %% "akka-persistence"     % version
    val `persistence-tck` = "com.typesafe.akka" %% "akka-persistence-tck" % version
    val slf4j             = "com.typesafe.akka" %% "akka-slf4j"           % version
  }
  
  object Kafka {
    private val version = "2.4.0"
    val kafka           = "org.apache.kafka" %% "kafka"         % version
    val `kafka-clients` = "org.apache.kafka" %  "kafka-clients" % version
  }

  object Scodec {
    val core = "org.scodec" %% "scodec-core" % "1.11.4"
    val bits = "org.scodec" %% "scodec-bits" % "1.1.14"
  }

  object Smetrics {
    private val version = "0.1.0"
    val smetrics   = "com.evolutiongaming" %% "smetrics"            % version
    val prometheus = "com.evolutiongaming" %% "smetrics-prometheus" % version
  }

  object Pureconfig {
    private val version = "0.12.3"
    val pureconfig = "com.github.pureconfig" %% "pureconfig"      % version
    val cats       = "com.github.pureconfig" %% "pureconfig-cats" % version
  }
}