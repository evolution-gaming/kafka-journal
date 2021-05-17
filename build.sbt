import Dependencies._
import sbt.librarymanagement.MavenRepository

lazy val commonSettings = Seq(
  organization := "com.evolutiongaming",
  homepage := Some(new URL("http://github.com/evolution-gaming/kafka-journal")),
  startYear := Some(2018),
  organizationName := "Evolution Gaming",
  organizationHomepage := Some(url("http://evolutiongaming.com")),
  publishTo := Some(Resolver.evolutionReleases),
  scalaVersion := crossScalaVersions.value.head,
  crossScalaVersions := Seq("2.13.6", "2.12.10"),
  Compile / doc / scalacOptions ++= Seq("-groups", "-implicits", "-no-link-warnings"),
  scalacOptsFailOnWarn := Some(false),
  licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT"))),
  releaseCrossBuild := true,
  Test / testOptions ++= Seq(Tests.Argument(TestFrameworks.ScalaTest, "-oUDNCXEHLOPQRM")),
  libraryDependencies += compilerPlugin(`kind-projector` cross CrossVersion.full))


lazy val root = (project in file(".")
  settings (name := "kafka-journal")
  settings commonSettings
  settings (publish / skip  := true)
  aggregate(
    `scalatest-io`,
    journal,
    persistence,
    `tests`,
    replicator,
    `eventual-cassandra`,
    `journal-circe`,
    `persistence-circe`))

lazy val `scalatest-io` = (project in file("scalatest-io")
  settings (name := "kafka-journal-scalatest-io")
  settings commonSettings
  settings (publish / skip  := true)
  settings (libraryDependencies ++= Seq(
    scalatest,
    Smetrics.smetrics,
    Cats.core,
    Cats.effect)))

lazy val journal = (project in file("journal")
  settings (name := "kafka-journal")
  settings commonSettings
  dependsOn (`scalatest-io` % Test)
  settings (libraryDependencies ++= Seq(
    Akka.actor,
    Akka.stream,
    Akka.testkit % Test,
    Akka.slf4j % Test,
    `cats-helper`,
    Kafka.`kafka-clients`,
    skafka,
    scalatest % Test,
    `executor-tools`,
    random,
    retry,
    `cats-helper`,
    `play-json`,
    `play-json-jsoniter`,
    `future-helper`,
    hostname,
    `cassandra-driver`,
    scassandra,
    scache,
    `cassandra-sync`,
    `scala-java8-compat`,
    Pureconfig.pureconfig,
    Pureconfig.cats,
    Smetrics.smetrics,
    sstream,
    Cats.core,
    Cats.effect,
    Scodec.core,
    Scodec.bits,
    Logback.core % Test,
    Logback.classic % Test)))

lazy val persistence = (project in file("persistence")
  settings (name := "kafka-journal-persistence")
  settings commonSettings
  dependsOn (
    journal % "test->test;compile->compile", 
    `eventual-cassandra`)
  settings (libraryDependencies ++= Seq(
    `akka-serialization`,
    `cats-helper`,
    Akka.persistence,
    `akka-test-actor` % Test)))

lazy val `tests` = (project in file("tests")
  settings (name := "kafka-journal-tests")
  settings commonSettings
  settings Seq(
    publish / skip  := true,
    Test / fork := true,
    Test / parallelExecution := false,
    Test / javaOptions ++= Seq("-Xms3G", "-Xmx3G"))
  dependsOn (
    persistence % "test->test;compile->compile",
    `persistence-circe`,
    replicator)
  settings (libraryDependencies ++= Seq(
    `cats-helper`,
    Kafka.kafka % Test,
    `kafka-launcher` % Test,
    `cassandra-launcher` % Test,
    scalatest % Test,
    Akka.`persistence-tck` % Test,
    Slf4j.`log4j-over-slf4j` % Test,
    Logback.core % Test,
    Logback.classic % Test,
    scalatest % Test)))

lazy val replicator = (Project("replicator", file("replicator"))
  settings (name := "kafka-journal-replicator")
  settings commonSettings
  dependsOn (
    journal % "test->test;compile->compile", 
    `eventual-cassandra`)
  settings (libraryDependencies ++= Seq(`cats-helper`)))

lazy val `eventual-cassandra` = (project in file("eventual-cassandra")
  settings (name := "kafka-journal-eventual-cassandra")
  settings commonSettings
  dependsOn (journal % "test->test;compile->compile")
  settings (libraryDependencies ++= Seq(scache, scassandra)))

lazy val `journal-circe` = (project in file("circe/core")
  settings (name := "kafka-journal-circe")
  settings commonSettings
  dependsOn (journal % "test->test;compile->compile")
  settings (libraryDependencies ++= Seq(Circe.core, Circe.generic, Circe.jawn)))

lazy val `persistence-circe` = (project in file("circe/persistence")
  settings (name := "kafka-journal-persistence-circe")
  settings commonSettings
  dependsOn (`journal-circe`, persistence % "test->test;compile->compile"))