import Dependencies._

lazy val commonSettings = Seq(
  organization := "com.evolutiongaming",
  homepage := Some(new URL("http://github.com/evolution-gaming/kafka-journal")),
  startYear := Some(2018),
  organizationName := "Evolution Gaming",
  organizationHomepage := Some(url("http://evolutiongaming.com")),
  bintrayOrganization := Some("evolutiongaming"),
  scalaVersion := crossScalaVersions.value.head,
  crossScalaVersions := Seq(/*"2.13.0", */"2.12.10"),
  scalacOptions in(Compile, doc) ++= Seq("-groups", "-implicits", "-no-link-warnings"),
  scalacOptsFailOnWarn := Some(false),
  resolvers += Resolver.bintrayRepo("evolutiongaming", "maven"),
  licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT"))),
  releaseCrossBuild := true,
  testOptions in Test ++= Seq(Tests.Argument(TestFrameworks.ScalaTest, "-oUDNCXEHLOPQRM")),
  libraryDependencies += compilerPlugin(`kind-projector` cross CrossVersion.binary))


lazy val root = (project in file(".")
  settings (name := "kafka-journal")
  settings commonSettings
  settings (skip in publish := true)
  aggregate(
    `scalatest-io`,
    journal,
    persistence,
    `tests`,
    replicator,
    `eventual-cassandra`))

lazy val `scalatest-io` = (project in file("scalatest-io")
  settings (name := "kafka-journal-scalatest-io")
  settings commonSettings
  settings (skip in publish := true)
  settings (libraryDependencies ++= Seq(
    scalatest,
    Smetrics.smetrics,
    Cats.core,
    Cats.effect)))

lazy val journal = (project in file("journal")
  settings (name := "kafka-journal")
  settings commonSettings
  dependsOn (`scalatest-io` % "test->compile")
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