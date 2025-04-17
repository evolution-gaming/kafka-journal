import Dependencies.*
import com.typesafe.tools.mima.core.*
import sbt.Package.ManifestAttributes

def crossSettings[T](scalaVersion: String, if3: T, if2: T): T = {
  scalaVersion match {
    case version if version.startsWith("3") => if3
    case _ => if2
  }
}

lazy val commonSettings = Seq(
  organization := "com.evolutiongaming",
  organizationName := "Evolution",
  organizationHomepage := Some(url("https://evolution.com")),
  homepage := Some(url("https://github.com/evolution-gaming/kafka-journal")),
  startYear := Some(2018),
  crossScalaVersions := Seq("2.13.16", "3.3.5"),
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
  // add mima check exceptions here, i.e.:
  //
  //    // TODO: [4.2.0 release] remove
  //    // package-private method change
  //    ProblemFilters.exclude[IncompatibleMethTypeProblem](
  //      "com.evolutiongaming.kafka.journal.replicator.TopicReplicator#ConsumerOf.make",
  //    ),
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
    `scalatest-io`,
    core,
    journal,
    snapshot,
    persistence,
    `tests`,
    replicator,
    cassandra,
    `eventual-cassandra`,
    `snapshot-cassandra`,
    `journal-circe`,
    `persistence-circe`,
  )

lazy val `scalatest-io` = project
  .settings(name := "kafka-journal-scalatest-io")
  .settings(commonSettings)
  .settings(publish / skip := true)
  .settings(libraryDependencies ++= Seq(scalatest, Smetrics.smetrics, `cats-helper`, Cats.core, Cats.effect))

lazy val core = project
  .settings(name := "kafka-journal-core")
  .settings(commonSettings)
  .dependsOn(`scalatest-io` % Test)
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
    ),
    libraryDependencies ++= crossSettings(
      scalaVersion = scalaVersion.value,
      if2 = Seq(
        Scodec.Scala2.core,
        Scodec.Scala2.bits,
      ),
      if3 = Seq(
        Scodec.Scala3.core,
        Scodec.Scala3.bits,
      ),
    ),
  )

lazy val journal = project
  .settings(name := "kafka-journal")
  .settings(commonSettings)
  .dependsOn(core % "test->test;compile->compile", `scalatest-io` % Test)
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

lazy val snapshot = project
  .settings(name := "kafka-journal-snapshot")
  .settings(commonSettings)
  .dependsOn(core)
  .settings(libraryDependencies ++= Seq(scalatest % Test))

lazy val persistence = project
  .settings(name := "kafka-journal-persistence")
  .settings(commonSettings)
  .dependsOn(journal % "test->test;compile->compile", `eventual-cassandra`, `snapshot-cassandra`)
  .settings(
    libraryDependencies ++= Seq(
      `akka-serialization`,
      `cats-helper`,
      Akka.persistence,
      `akka-test-actor` % Test,
    ),
  )

lazy val `tests` = project
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
  .dependsOn(persistence % "test->test;compile->compile", `persistence-circe`, replicator)
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

lazy val replicator = project
  .settings(name := "kafka-journal-replicator")
  .settings(commonSettings)
  .dependsOn(journal % "test->test;compile->compile", `eventual-cassandra`)
  .settings(libraryDependencies ++= Seq(`cats-helper`, Logback.core % Test, Logback.classic % Test))

lazy val cassandra = project
  .settings(name := "kafka-journal-cassandra")
  .settings(commonSettings)
  .dependsOn(core, `scalatest-io` % Test)
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

lazy val `eventual-cassandra` = project
  .settings(name := "kafka-journal-eventual-cassandra")
  .settings(commonSettings)
  .dependsOn(cassandra % "test->test;compile->compile", journal % "test->test;compile->compile")
  .settings(libraryDependencies ++= Seq(scassandra))

lazy val `snapshot-cassandra` = project
  .settings(name := "kafka-journal-snapshot-cassandra")
  .settings(commonSettings)
  .dependsOn(cassandra, snapshot % "test->test;compile->compile")
  .settings(libraryDependencies ++= Seq(scassandra))

lazy val `journal-circe` = project
  .in(file("circe/core"))
  .settings(name := "kafka-journal-circe")
  .settings(commonSettings)
  .dependsOn(journal % "test->test;compile->compile")
  .settings(libraryDependencies ++= Seq(Circe.core, Circe.generic, Circe.jawn))

lazy val `persistence-circe` = project
  .in(file("circe/persistence"))
  .settings(name := "kafka-journal-persistence-circe")
  .settings(commonSettings)
  .dependsOn(`journal-circe`, persistence % "test->test;compile->compile")

lazy val benchmark = project
  .dependsOn(replicator % "test->test", journal % "test->test;compile->compile", `eventual-cassandra`)
  .enablePlugins(JmhPlugin)
  .settings(commonSettings)
  .settings(
    Jmh / sourceDirectory := (Test / sourceDirectory).value,
    Jmh / classDirectory := (Test / classDirectory).value,
    Jmh / dependencyClasspath := (Test / dependencyClasspath).value,
  )
