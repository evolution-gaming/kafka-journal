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
    journal,
    persistence, `persistence-tests`,
    replicator))

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
    Logback,
    Cassandra,
    PubSub,
    PlayJson,
    ScalaTools)))

lazy val persistence = (project in file("persistence")
  settings (name := "kafka-journal-persistence")
  settings commonSettings
  dependsOn journal % "test->test;compile->compile"
  settings (libraryDependencies ++= Seq()))

lazy val `persistence-tests` = (project in file("persistence-tests")
  settings (name := "kafka-journal-persistence-tests")
  settings commonSettings
  settings Seq(
    skip in publish := true,
    //    Test / testOptions in Test := Seq(Tests.Filter(_ => false)),
    Test / fork := true,
    Test / parallelExecution := false)
  dependsOn persistence % "test->test;compile->compile"
  settings (libraryDependencies ++= Seq(Kafka.Server)))

lazy val replicator = (Project("replicator", file("replicator"))
  settings (name := "kafka-journal-replicator")
  settings commonSettings
  dependsOn journal % "test->test;compile->compile"
  settings (libraryDependencies ++= Seq()))

lazy val `cassandra-client` = (project in file("cassandra-client")
  settings (name := "kafka-journal-cassandra-client")
  settings commonSettings
  settings (libraryDependencies ++= Seq(
    Cassandra,
    ConfigTools,
    ScalaTest)))