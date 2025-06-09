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
    if2 = Seq(
      compilerPlugin(`kind-projector` cross CrossVersion.full),
    ),
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
    akkaCore,
    akkaJournal,
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

lazy val akkaCore = project
  .in(file("akka/core"))
  .settings(name := "kafka-journal-core")
  .settings(commonSettings)
  .dependsOn(ScalaTestIO % Test)
  .settings(
    libraryDependencies ++= Seq(
      Akka.actor,
      Akka.testkit % Test,
      skafka,
      `cats-helper`,
      `play-json`,
      `play-json-jsoniter`,
      scassandra,
      hostname,
      Cats.core,
      Cats.effect,
      Scodec.bits,
    ),
    libraryDependencies ++= crossSettings(
      scalaVersion = scalaVersion.value,
      if2 = Seq(Scodec.Scala2.core),
      if3 = Seq(Scodec.Scala3.core),
    ),
  )

lazy val akkaJournal = project
  .in(file("akka/journal"))
  .settings(name := "kafka-journal")
  .settings(commonSettings)
  .dependsOn(akkaCore % "test->test;compile->compile", ScalaTestIO % Test)
  .settings(
    libraryDependencies ++= Seq(
      Akka.actor,
      Akka.stream,
      Akka.testkit % Test,
      Akka.slf4j % Test,
      `kafka-clients`,
      skafka,
      scalatest % Test,
      `executor-tools` % Test,
      random,
      retry,
      `cats-helper`,
      `play-json`,
      `play-json-jsoniter`,
      hostname,
      `cassandra-driver`,
      scassandra,
      scache,
      `cassandra-sync`,
      `scala-java8-compat`,
      Pureconfig.core,
      Pureconfig.cats,
      Smetrics.smetrics,
      sstream,
      Cats.core,
      Cats.effect,
      `resource-pool`,
      Logback.core % Test,
      Logback.classic % Test,
    ),
    libraryDependencies ++= crossSettings(
      scalaVersion = scalaVersion.value,
      if2 = Seq(),
      if3 = Seq(Pureconfig.`generic-scala3`),
    ),
  )

lazy val akkaSnapshot = project
  .in(file("akka/snapshot"))
  .settings(name := "kafka-journal-snapshot")
  .settings(commonSettings)
  .dependsOn(akkaCore)
  .settings(libraryDependencies ++= Seq(scalatest % Test))

lazy val akkaPersistence = project
  .in(file("akka/persistence"))
  .settings(name := "kafka-journal-persistence")
  .settings(commonSettings)
  .dependsOn(akkaJournal % "test->test;compile->compile", akkaEventualCassandra, akkaSnapshotCassandra)
  .settings(
    libraryDependencies ++= Seq(
      `akka-serialization`,
      `cats-helper`,
      Akka.persistence,
      `akka-test-actor` % Test,
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
      `cats-helper`,
      TestContainers.cassandra % Test,
      TestContainers.kafka % Test,
      scalatest % Test,
      Akka.`persistence-tck` % Test,
      Slf4j.`log4j-over-slf4j` % Test,
      Logback.core % Test,
      Logback.classic % Test,
      scalatest % Test,
    ),
  )

lazy val akkaReplicator = project
  .in(file("akka/replicator"))
  .settings(name := "kafka-journal-replicator")
  .settings(commonSettings)
  .dependsOn(akkaJournal % "test->test;compile->compile", akkaEventualCassandra)
  .settings(libraryDependencies ++= Seq(`cats-helper`, Logback.core % Test, Logback.classic % Test))

lazy val akkaCassandra = project
  .in(file("akka/cassandra"))
  .settings(name := "kafka-journal-cassandra")
  .settings(commonSettings)
  .dependsOn(akkaCore, ScalaTestIO % Test)
  .settings(
    libraryDependencies ++= Seq(
      scache,
      scassandra,
      `cassandra-sync`,
    ),
    libraryDependencies ++= crossSettings(
      scalaVersion = scalaVersion.value,
      if2 = Seq(),
      if3 = Seq(Pureconfig.`generic-scala3`),
    ),
  )

lazy val akkaEventualCassandra = project
  .in(file("akka/eventual-cassandra"))
  .settings(name := "kafka-journal-eventual-cassandra")
  .settings(commonSettings)
  .dependsOn(akkaCassandra % "test->test;compile->compile", akkaJournal % "test->test;compile->compile")
  .settings(libraryDependencies ++= Seq(scassandra))

lazy val akkaSnapshotCassandra = project
  .in(file("akka/snapshot-cassandra"))
  .settings(name := "kafka-journal-snapshot-cassandra")
  .settings(commonSettings)
  .dependsOn(akkaCassandra, akkaSnapshot % "test->test;compile->compile")
  .settings(libraryDependencies ++= Seq(scassandra))

lazy val akkaJournalCirce = project
  .in(file("akka/circe/core"))
  .settings(name := "kafka-journal-circe")
  .settings(commonSettings)
  .dependsOn(akkaJournal % "test->test;compile->compile")
  .settings(libraryDependencies ++= Seq(Circe.core, Circe.generic, Circe.jawn))

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
  .settings(libraryDependencies ++= Seq(scalatest, Smetrics.smetrics, `cats-helper`, Cats.core, Cats.effect))

// not part of aggregate, tests can be run only manually
lazy val benchmark = project
  .dependsOn(akkaJournal % "test->test;compile->compile")
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
