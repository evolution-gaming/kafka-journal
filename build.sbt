import Dependencies._

lazy val commonSettings = Seq(
  organization := "com.evolutiongaming",
  homepage := Some(new URL("http://github.com/evolution-gaming/kafka-journal")),
  startYear := Some(2018),
  organizationName := "Evolution Gaming",
  organizationHomepage := Some(url("http://evolutiongaming.com")),
  bintrayOrganization := Some("evolutiongaming"),
  scalaVersion := crossScalaVersions.value.last,
  crossScalaVersions := Seq("2.11.12", "2.12.6"),
  scalacOptions ++= Seq(
    "-encoding", "UTF-8",
    "-feature",
    "-unchecked",
    "-deprecation",
    //    "-Xfatal-warnings", TODO
    "-Xlint",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Xfuture"),
  scalacOptions in(Compile, doc) ++= Seq("-groups", "-implicits", "-no-link-warnings"),
  resolvers += Resolver.bintrayRepo("evolutiongaming", "maven"),
  licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT"))),
  releaseCrossBuild := true)


lazy val root = (project in file(".")
  settings (name := "kafka-journal")
  settings commonSettings
  settings (skip in publish := true)
  aggregate(
    `cassandra-client`,
    `cassandra-launcher`,
    journal,
    persistence,
    `persistence-tests`,
    replicator,
    `tmp-dir`,
    `kafka-launcher`,
    `eventual-cassandra`))

// TODO cleanup dependencies
lazy val journal = (project in file("journal")
  settings (name := "kafka-journal")
  settings commonSettings
  dependsOn `cassandra-client`
  settings (libraryDependencies ++= Seq(
    Akka.Persistence,
    Akka.Tck,
    Akka.Slf4j,
    Skafka,
    Kafka.Clients,
    ScalaTest,
    ExecutorTools,
    Logback.Core % Test,
    Cassandra.Driver,
    PubSub,
    PlayJson,
    ScalaTools)))

lazy val persistence = (project in file("persistence")
  settings (name := "kafka-journal-persistence")
  settings commonSettings
  dependsOn (journal % "test->test;compile->compile", `eventual-cassandra`)
  settings (libraryDependencies ++= Seq()))

lazy val `persistence-tests` = (project in file("persistence-tests")
  settings (name := "kafka-journal-persistence-tests")
  settings commonSettings
  settings Seq(
    skip in publish := true, // TODO use in other modules
    Test / testOptions in Test := Seq(Tests.Filter(_ endsWith "IntegrationSpec")),
    Test / fork := true,
    Test / parallelExecution := false)
  dependsOn (
    persistence % "test->test;compile->compile",
    `kafka-launcher`,
    `cassandra-launcher`,
    replicator)
  settings (libraryDependencies ++= Seq()))

lazy val replicator = (Project("replicator", file("replicator"))
  settings (name := "kafka-journal-replicator")
  settings commonSettings
  dependsOn (journal % "test->test;compile->compile", `eventual-cassandra`)
  settings (libraryDependencies ++= Seq()))

// TODO rename to scassandra
lazy val `cassandra-client` = (project in file("cassandra-client")
  settings (name := "cassandra-client")
  settings commonSettings
  dependsOn (`cassandra-launcher` % "compile->test")
  settings (libraryDependencies ++= Seq(
    Cassandra.Driver,
    ConfigTools,
    ScalaTest,
    Nel,
    ExecutorTools,
    Skafka)))

lazy val `tmp-dir` = (project in file("tmp-dir")
  settings (name := "tmp-dir")
  settings commonSettings
  settings (libraryDependencies ++= Seq(CommonsIo)))

lazy val `cassandra-launcher` = (project in file("cassandra-launcher")
  settings (name := "cassandra-launcher")
  settings commonSettings
  dependsOn `tmp-dir`
  settings (libraryDependencies ++= Seq(
    Cassandra.Server,
    Slf4j.Api % Test,
    Slf4j.OverLog4j % Test,
    Logback.Core % Test,
    Logback.Classic % Test,
    ScalaTest)))

lazy val `kafka-launcher` = (project in file("kafka-launcher")
  settings (name := "kafka-launcher")
  settings commonSettings
  dependsOn `tmp-dir`
  settings (libraryDependencies ++= Seq(Kafka.Server, ScalaTest)))

lazy val `eventual-cassandra` = (project in file("eventual-cassandra")
  settings (name := "kafka-journal-eventual-cassandra")
  settings commonSettings
  dependsOn (`cassandra-client`, journal % "test->test;compile->compile")
  settings (libraryDependencies ++= Seq()))