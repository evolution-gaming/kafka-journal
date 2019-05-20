import Dependencies._

lazy val commonSettings = Seq(
  organization := "com.evolutiongaming",
  homepage := Some(new URL("http://github.com/evolution-gaming/kafka-journal")),
  startYear := Some(2018),
  organizationName := "Evolution Gaming",
  organizationHomepage := Some(url("http://evolutiongaming.com")),
  bintrayOrganization := Some("evolutiongaming"),
  scalaVersion := crossScalaVersions.value.head,
  crossScalaVersions := Seq("2.12.8"),
  scalacOptions in(Compile, doc) ++= Seq("-groups", "-implicits", "-no-link-warnings"),
  resolvers += Resolver.bintrayRepo("evolutiongaming", "maven"),
  licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT"))),
  releaseCrossBuild := true,
  testOptions in Test ++= Seq(Tests.Argument(TestFrameworks.ScalaTest, "-oUDNCXEHLOPQRM")))


lazy val root = (project in file(".")
  settings (name := "kafka-journal")
  settings commonSettings
  settings (skip in publish := true)
  aggregate(
    `scalatest-io`,
    `cats-effect-helpers`,
    cache,
    stream,
    journal,
    `journal-prometheus`,
    persistence,
    `tests`,
    replicator,
    `replicator-prometheus`,
    `eventual-cassandra`))

lazy val `scalatest-io` = (project in file("scalatest-io")
  settings (name := "kafka-journal-scalatest-io")
  settings commonSettings
  settings (skip in publish := true)
  settings (libraryDependencies ++= Seq(
    scalatest,
    Cats.core,
    Cats.effect)))

lazy val stream = (project in file("stream")
  settings (name := "kafka-journal-stream")
  settings commonSettings
  settings (libraryDependencies ++= Seq(
    scalatest % Test,
    Cats.core,
    Cats.effect)))

lazy val `cats-effect-helpers` = (project in file("cats-effect-helpers")
  settings (name := "kafka-journal-cats-effect-helpers")
  settings commonSettings
  dependsOn `scalatest-io` % "test->compile"
  settings (libraryDependencies ++= Seq(
    `cats-helper`,
    Cats.core,
    Cats.effect,
    scalatest % Test)))

lazy val cache = (project in file("cache")
  settings (name := "kafka-journal-cache")
  settings commonSettings
  dependsOn (
    `cats-effect-helpers`, 
    `scalatest-io` % "test->compile")
  settings (libraryDependencies ++= Seq(
    scalatest % Test,
    Cats.core,
    Cats.effect)))

lazy val journal = (project in file("journal")
  settings (name := "kafka-journal")
  settings commonSettings
  dependsOn (stream, cache, `scalatest-io` % "test->compile")
  settings (libraryDependencies ++= Seq(
    Akka.actor,
    Akka.stream,
    Akka.testkit % Test,
    Akka.slf4j % Test,
    Kafka.`kafka-clients`,
    Skafka.skafka,
    scalatest % Test,
    `executor-tools`,
    random,
    retry,
    `cats-helper`,
    `play-json`,
    `future-helper`,
    hostname,
    `cassandra-driver`,
    scassandra,
    `cassandra-sync`,
    `scala-java8-compat`,
    `cats-par`,
    pureconfig,
    Cats.core,
    Cats.effect,
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
    Scodec.core,
    Scodec.bits,
    Akka.persistence)))

lazy val `tests` = (project in file("tests")
  settings (name := "kafka-journal-tests")
  settings commonSettings
  settings Seq(
    skip in publish := true,
    Test / fork := true,
    Test / parallelExecution := false)
  dependsOn (
    persistence % "test->test;compile->compile",
    replicator)
  settings (libraryDependencies ++= Seq(
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
  settings (libraryDependencies ++= Seq()))

lazy val `eventual-cassandra` = (project in file("eventual-cassandra")
  settings (name := "kafka-journal-eventual-cassandra")
  settings commonSettings
  dependsOn (journal % "test->test;compile->compile")
  settings (libraryDependencies ++= Seq(scassandra)))

lazy val `journal-prometheus` = (project in file("journal-prometheus")
  settings (name := "kafka-journal-prometheus")
  settings commonSettings
  dependsOn journal % "test->test;compile->compile"
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