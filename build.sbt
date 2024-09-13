import Dependencies.*
import sbt.Package.ManifestAttributes

lazy val commonSettings = Seq(
  organization := "com.evolutiongaming",
  organizationName := "Evolution",
  organizationHomepage := Some(url("https://evolution.com")),
  homepage := Some(url("https://github.com/evolution-gaming/kafka-journal")),
  startYear := Some(2018),
  crossScalaVersions := Seq("2.13.14"),
  scalaVersion := crossScalaVersions.value.head,
  scalacOptions ++= Seq("-release:17", "-deprecation", "-Xsource:3"),
  Compile / doc / scalacOptions ++= Seq("-groups", "-implicits", "-no-link-warnings"),
  Compile / doc / scalacOptions -= "-Xfatal-warnings",
  publishTo := Some(Resolver.evolutionReleases),
  licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT"))),
  releaseCrossBuild := true,
  // explanation of flags: https://www.scalatest.org/user_guide/using_the_runner (`Configuring reporters` section)
  Test / testOptions ++= Seq(Tests.Argument(TestFrameworks.ScalaTest, "-oUDNCXEHLOPQRM")),
  libraryDependencies += compilerPlugin(`kind-projector` cross CrossVersion.full),
  libraryDependencySchemes ++= Seq(
    "org.scala-lang.modules" %% "scala-java8-compat" % "always",
    "org.scala-lang.modules" %% "scala-xml"          % "always",
    "com.evolutiongaming"    %% "scassandra"         % "semver-spec",
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

import com.typesafe.tools.mima.core.*
ThisBuild / mimaBinaryIssueFilters ++= Seq(
  ProblemFilters.exclude[DirectMissingMethodProblem](
    "com.evolutiongaming.kafka.journal.replicator.TopicReplicatorMetrics.append",
  ),
  ProblemFilters.exclude[ReversedMissingMethodProblem](
    "com.evolutiongaming.kafka.journal.replicator.TopicReplicatorMetrics.append",
  ),
)

val alias: Seq[sbt.Def.Setting[?]] =
  addCommandAlias("fmt", "all scalafmtAll scalafmtSbt; scalafixEnable; scalafixAll") ++
    addCommandAlias(
      "check",
      "all versionPolicyCheck Compile/doc scalafmtCheckAll scalafmtSbtCheck; scalafixEnable; scalafixAll --check",
    ) ++
    addCommandAlias("build", "all compile test")

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
      Scodec.core,
      Scodec.bits,
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
      Akka.slf4j   % Test,
      Kafka.`kafka-clients`,
      skafka,
      scalatest        % Test,
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
      Pureconfig.pureconfig,
      Pureconfig.cats,
      Smetrics.smetrics,
      sstream,
      Cats.core,
      Cats.effect,
      Scodec.core,
      Scodec.bits,
      `resource-pool`,
      Logback.core    % Test,
      Logback.classic % Test,
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
      Test / envVars ++= Map("TESTCONTAINERS_RYUK_DISABLED" -> "true"),
    ),
  )
  .dependsOn(persistence % "test->test;compile->compile", `persistence-circe`, replicator)
  .settings(
    libraryDependencies ++= Seq(
      `cats-helper`,
      Kafka.kafka                % Test,
      `kafka-launcher`           % Test,
      `testcontainers-cassandra` % Test,
      scalatest                  % Test,
      Akka.`persistence-tck`     % Test,
      Slf4j.`log4j-over-slf4j`   % Test,
      Logback.core               % Test,
      Logback.classic            % Test,
      scalatest                  % Test,
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
  .settings(libraryDependencies ++= Seq(scache, scassandra, `cassandra-sync`))

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
