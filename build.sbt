import Dependencies.*
import com.typesafe.tools.mima.core.*
import sbt.Package.ManifestAttributes

lazy val commonSettings = Seq(
  organization := "com.evolutiongaming",
  organizationName := "Evolution",
  organizationHomepage := Some(url("https://evolution.com")),
  homepage := Some(url("https://github.com/evolution-gaming/kafka-journal")),
  startYear := Some(2018),
  crossScalaVersions := Seq("2.13.16", "3.3.6"),
  scalaVersion := crossScalaVersions.value.head,
  scalacOptions ++= Seq(
    "-release:17",
    "-deprecation",
  ),
  scalacOptions ++= crossSettings(
    scalaVersion = scalaVersion.value,
    // Good compiler options for Scala 2.13 are coming from com.evolution:sbt-scalac-opts-plugin:0.0.9,
    // but its support for Scala 3 is limited, especially what concerns linting options.
    //
    // If Scala 3 is made the primary target, good linting scalac options for it should be added first.
    if3 = Seq(
      "-Ykind-projector:underscores",

      // disable new brace-less syntax:
      // https://alexn.org/blog/2022/10/24/scala-3-optional-braces/
      "-no-indent",

      // improve error messages:
      "-explain",
      "-explain-types",
    ),
    if2 = Seq(
      "-Xsource:3",
    ),
  ),
  Compile / doc / scalacOptions ++= Seq("-groups", "-implicits", "-no-link-warnings"),
  Compile / doc / scalacOptions -= "-Xfatal-warnings",
  publishTo := Some(Resolver.evolutionReleases),
  licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT"))),
  // set up compiler plugins:
  libraryDependencies ++= crossSettings(
    scalaVersion = scalaVersion.value,
    if3 = Seq(),
    if2 = Seq(compilerPlugin(KindProjector cross CrossVersion.full)),
  ),
  libraryDependencySchemes ++= Seq(
    "org.scala-lang.modules" %% "scala-java8-compat" % "always",
    "org.scala-lang.modules" %% "scala-xml" % "always",
  ),
  autoAPIMappings := true,
  versionScheme := Some("early-semver"),
  versionPolicyIntention := Compatibility.BinaryCompatible,
  packageOptions := {
    Seq(
      ManifestAttributes(
        ("Implementation-Version", (ThisProject / version).value),
      ),
    )
  },
)

ThisBuild / mimaBinaryIssueFilters ++= Seq(
  // add mima check exceptions here, like:
//  ProblemFilters.exclude[IncompatibleMethTypeProblem](
//    "com.evolutiongaming.kafka.journal.replicator.TopicReplicator#ConsumerOf.make",
//  ),
)

val alias: Seq[sbt.Def.Setting[?]] =
  addCommandAlias("fmt", "+all scalafmtAll scalafmtSbt") ++
    addCommandAlias(
      "check", // check is called with + from the release action
      "all versionPolicyCheck Compile/doc scalafmtCheckAll scalafmtSbtCheck",
    ) ++
    addCommandAlias("build", "+all compile test")

lazy val root = project
  .in(file("."))
  .settings(name := "kafka-journal")
  .settings(commonSettings)
  .settings(publish / skip := true)
  .settings(alias)
  .aggregate(
    core,
    journal,
    akkaSnapshot,
    akkaPersistence,
    akkaTests,
    akkaReplicator,
    akkaCassandra,
    akkaEventualCassandra,
    akkaSnapshotCassandra,
    akkaJournalCirce,
    akkaPersistenceCirce,
    ScalaTestIO,
  )

lazy val core = project
  .in(file("core"))
  .settings(name := "kafka-journal-core")
  .settings(commonSettings)
  .dependsOn(ScalaTestIO % Test)
  .settings(
    libraryDependencies ++= Seq(
      SKafka,
      CatsHelper,
      PlayJson,
      PlayJsonJsoniter,
      SCassandra, // TODO MR remove
      Hostname,
      Cats.Core,
      Cats.Effect,
      Scodec.Bits,
    ),
    libraryDependencies ++= crossSettings(
      scalaVersion = scalaVersion.value,
      if2 = Seq(Scodec.Scala2.Core),
      if3 = Seq(Scodec.Scala3.Core),
    ),
  )

lazy val journal = project
  .in(file("journal"))
  .settings(name := "kafka-journal")
  .settings(commonSettings)
  .dependsOn(core % "test->test;compile->compile", ScalaTestIO % Test)
  .settings(
    libraryDependencies ++= Seq(
      KafkaClients,
      SKafka,
      ScalaTest % Test,
      ExecutorTools % Test,
      Random,
      Retry,
      CatsHelper,
      PlayJson,
      PlayJsonJsoniter,
      Hostname,
      CassandraDriver,
      SCassandra, // TODO MR remove
      SCache,
      CassandraSync, // TODO MR remove
      ScalaJava8Compat,
      Pureconfig.Core,
      Pureconfig.Cats,
      Smetrics.SMetrics,
      SStream,
      Cats.Core,
      Cats.Effect,
      ResourcePool,
      Logback.Core % Test,
      Logback.Classic % Test,
    ),
    libraryDependencies ++= crossSettings(
      scalaVersion = scalaVersion.value,
      if2 = Seq(),
      if3 = Seq(Pureconfig.Scala3.Generic),
    ),
  )

lazy val akkaSnapshot = project
  .in(file("akka/snapshot"))
  .settings(name := "kafka-journal-snapshot")
  .settings(commonSettings)
  .dependsOn(core)
  .settings(libraryDependencies ++= Seq(ScalaTest % Test))

lazy val akkaPersistence = project
  .in(file("akka/persistence"))
  .settings(name := "kafka-journal-persistence")
  .settings(commonSettings)
  .dependsOn(journal % "test->test;compile->compile", akkaEventualCassandra, akkaSnapshotCassandra)
  .settings(
    libraryDependencies ++= Seq(
      AkkaSerialization,
      CatsHelper,
      Akka.Persistence,
      Akka.Testkit % Test,
      AkkaTestActor % Test,
    ),
  )

lazy val akkaTests = project
  .in(file("akka/tests"))
  .settings(name := "kafka-journal-tests")
  .settings(commonSettings)
  .settings(
    Seq(
      publish / skip := true,
      Test / fork := true,
      Test / parallelExecution := false,
      Test / javaOptions ++= Seq("-Xms3G", "-Xmx3G"),
    ),
  )
  .dependsOn(akkaPersistence % "test->test;compile->compile", akkaPersistenceCirce, akkaReplicator)
  .settings(
    libraryDependencies ++= Seq(
      CatsHelper,
      TestContainers.Cassandra % Test,
      TestContainers.Kafka % Test,
      ScalaTest % Test,
      Akka.PersistenceTck % Test,
      Akka.Slf4j % Test,
      Slf4j.Log4jOverSlf4j % Test,
      Logback.Core % Test,
      Logback.Classic % Test,
      ScalaTest % Test,
    ),
  )

lazy val akkaReplicator = project
  .in(file("akka/replicator"))
  .settings(name := "kafka-journal-replicator")
  .settings(commonSettings)
  .dependsOn(
    journal % "test->test",
    akkaEventualCassandra,
    ScalaTestIO % Test,
  )
  .settings(libraryDependencies ++= Seq(
    CatsHelper,
    Logback.Core % Test,
    Logback.Classic % Test,
    ScalaTest % Test,
  ))

lazy val akkaCassandra = project
  .in(file("akka/cassandra"))
  .settings(name := "kafka-journal-cassandra")
  .settings(commonSettings)
  .dependsOn(core, ScalaTestIO % Test)
  .settings(
    libraryDependencies ++= Seq(
      SCache,
      SCassandra,
      CassandraSync,
    ),
    libraryDependencies ++= crossSettings(
      scalaVersion = scalaVersion.value,
      if2 = Seq(),
      if3 = Seq(Pureconfig.Scala3.Generic),
    ),
  )

lazy val akkaEventualCassandra = project
  .in(file("akka/eventual-cassandra"))
  .settings(name := "kafka-journal-eventual-cassandra")
  .settings(commonSettings)
  .dependsOn(akkaCassandra % "test->test;compile->compile", journal % "test->test;compile->compile")
  .settings(libraryDependencies ++= Seq(SCassandra))

lazy val akkaSnapshotCassandra = project
  .in(file("akka/snapshot-cassandra"))
  .settings(name := "kafka-journal-snapshot-cassandra")
  .settings(commonSettings)
  .dependsOn(akkaCassandra, akkaSnapshot % "test->test;compile->compile")
  .settings(libraryDependencies ++= Seq(SCassandra))

lazy val akkaJournalCirce = project
  .in(file("akka/circe/core"))
  .settings(name := "kafka-journal-circe")
  .settings(commonSettings)
  .dependsOn(journal % "test->test;compile->compile")
  .settings(libraryDependencies ++= Seq(Circe.Core, Circe.Generic, Circe.Jawn))

lazy val akkaPersistenceCirce = project
  .in(file("akka/circe/persistence"))
  .settings(name := "kafka-journal-persistence-circe")
  .settings(commonSettings)
  .dependsOn(akkaJournalCirce, akkaPersistence % "test->test;compile->compile")

lazy val ScalaTestIO = project
  .in(file("scalatest-io"))
  .settings(name := "kafka-journal-scalatest-io")
  .settings(commonSettings)
  .settings(publish / skip := true)
  .settings(libraryDependencies ++= Seq(ScalaTest, Smetrics.SMetrics, CatsHelper, Cats.Core, Cats.Effect))

// not part of aggregate, tests can be run only manually
lazy val benchmark = project
  .dependsOn(journal % "test->test;compile->compile")
  .enablePlugins(JmhPlugin)
  .settings(commonSettings)
  .settings(
    Jmh / sourceDirectory := (Test / sourceDirectory).value,
    Jmh / classDirectory := (Test / classDirectory).value,
    Jmh / dependencyClasspath := (Test / dependencyClasspath).value,
  )

def crossSettings[T](scalaVersion: String, if3: T, if2: T): T = {
  scalaVersion match {
    case version if version.startsWith("3") => if3
    case _ => if2
  }
}
