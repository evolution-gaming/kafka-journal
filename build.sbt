import Dependencies._

lazy val commonSettings = Seq(
  organization := "com.evolutiongaming",
  homepage := Some(new URL("http://github.com/evolution-gaming/kafka-journal")),
  startYear := Some(2018),
  organizationName := "Evolution Gaming",
  organizationHomepage := Some(url("http://evolutiongaming.com")),
  bintrayOrganization := Some("evolutiongaming"),
  scalaVersion := crossScalaVersions.value.last,
  crossScalaVersions := Seq(/*"2.11.12", */"2.12.8"),
  scalacOptions ++= Seq(
    "-encoding", "UTF-8",
    "-feature",
    "-unchecked",
    "-deprecation",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Xfuture",
    "-Xlint",
    "-language:higherKinds",
    "-Ypartial-unification"),
  scalacOptions in(Compile, doc) ++= Seq("-groups", "-implicits", "-no-link-warnings"),
  resolvers += Resolver.bintrayRepo("evolutiongaming", "maven"),
  licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT"))),
  releaseCrossBuild := true)


lazy val root = (project in file(".")
  settings (name := "kafka-journal")
  settings commonSettings
  settings (skip in publish := true)
  aggregate(
    journal,
    `journal-prometheus`,
    persistence,
    `tests`,
    replicator,
    `replicator-prometheus`,
    `eventual-cassandra`))

lazy val journal = (project in file("journal")
  settings (name := "kafka-journal")
  settings commonSettings
  settings (libraryDependencies ++= Seq(
    Akka.actor,
    Akka.stream,
    Akka.testkit % Test,
    Akka.slf4j % Test,
    Skafka.skafka,
    scalatest % Test,
    `executor-tools`,
    Logback.core % Test,
    Logback.classic % Test,
    `play-json`,
    `future-helper`,
    `safe-actor`,
    hostname,
    scassandra,
    `cassandra-sync`,
    `scala-java8-compat`,
    Cats.core,
    Cats.effect)))

lazy val persistence = (project in file("persistence")
  settings (name := "kafka-journal-persistence")
  settings commonSettings
  dependsOn (journal % "test->test;compile->compile", `eventual-cassandra`)
  settings (libraryDependencies ++= Seq(
    `akka-serialization`,
    Akka.persistence)))

lazy val `tests` = (project in file("tests")
  settings (name := "kafka-journal-persistence-tests")
  settings commonSettings
  settings Seq(
    skip in publish := true,
    Test / fork := true,
    Test / parallelExecution := false)
  dependsOn (
    persistence % "test->test;compile->compile",
    replicator)
  settings (libraryDependencies ++= Seq(
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
  dependsOn (journal % "test->test;compile->compile", `eventual-cassandra`)
  settings (libraryDependencies ++= Seq()))

lazy val `eventual-cassandra` = (project in file("eventual-cassandra")
  settings (name := "kafka-journal-eventual-cassandra")
  settings commonSettings
  dependsOn (journal % "test->test;compile->compile")
  settings (libraryDependencies ++= Seq(scassandra)))

lazy val `journal-prometheus` = (project in file("journal-prometheus")
  settings (name := "kafka-journal-prometheus")
  settings commonSettings
  dependsOn journal
  settings (libraryDependencies ++= Seq(
    prometheus,
    scalatest % Test)))

lazy val `replicator-prometheus` = (project in file("replicator-prometheus")
  settings (name := "kafka-journal-replicator-prometheus")
  settings commonSettings
  dependsOn (replicator % "test->test;compile->compile")
  settings (libraryDependencies ++= Seq(
    prometheus,
    Skafka.prometheus,
    scalatest % Test)))