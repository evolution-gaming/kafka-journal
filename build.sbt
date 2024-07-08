import Dependencies.*
import sbt.Package.ManifestAttributes

ThisBuild / organization := "com.evolutiongaming"
ThisBuild / organizationName := "Evolution"
ThisBuild / organizationHomepage := Some(url("https://evolution.com"))
ThisBuild / homepage := Some(url("https://github.com/evolution-gaming/kafka-journal"))
ThisBuild / startYear := Some(2018)
ThisBuild / licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT")))
// ---
ThisBuild / crossScalaVersions := Seq("2.13.14")
ThisBuild / scalaVersion := crossScalaVersions.value.head
ThisBuild / scalacOptions ++= Seq("-release:17", "-deprecation", "-Xsource:3")
ThisBuild / autoAPIMappings := true
// ---
ThisBuild / Test / testOptions ++= Seq(Tests.Argument(TestFrameworks.ScalaTest, "-oUDNCXEHLOPQRM"))
ThisBuild / Test / fork := true
ThisBuild / Test / parallelExecution := false
// ---
ThisBuild / publishTo := Some(Resolver.evolutionReleases)
ThisBuild / releaseCrossBuild := true
ThisBuild / libraryDependencies += compilerPlugin(`kind-projector` cross CrossVersion.full)
ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-java8-compat" % "always",
  "org.scala-lang.modules" %% "scala-xml"          % "always",
  "com.evolutiongaming"    %% "scassandra"         % "semver-spec",
)
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / versionPolicyIntention := Compatibility.BinaryCompatible
// ---
ThisBuild / packageOptions := Seq(
  ManifestAttributes(
    ("Implementation-Version", (ThisProject / version).value),
  ),
)

lazy val docSettings = Seq(
  Compile / doc / scalacOptions ++= Seq("-groups", "-implicits", "-no-link-warnings"),
  Compile / doc / scalacOptions -= "-Xfatal-warnings",
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
  .settings(publish / skip := true)
  .settings(
    libraryDependencies ++= Seq(
      scalatest,
      Smetrics.smetrics,
      `cats-helper`,
      Cats.core,
      Cats.effect,
    ),
  )

lazy val core = project
  .settings(name := "kafka-journal-core")
  .dependsOn(`scalatest-io` % Test)
  .settings(docSettings)
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
  .dependsOn(core % "test->test;compile->compile", `scalatest-io` % Test)
  .settings(docSettings)
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
  .dependsOn(core)
  .settings(docSettings)
  .settings(libraryDependencies ++= Seq(scalatest % Test))

lazy val persistence = project
  .settings(name := "kafka-journal-persistence")
  .dependsOn(journal % "test->test;compile->compile", `eventual-cassandra`, `snapshot-cassandra`)
  .settings(docSettings)
  .settings(
    libraryDependencies ++= Seq(
      `akka-serialization`,
      `cats-helper`,
      Akka.persistence,
      `akka-test-actor` % Test,
    ),
  )

lazy val replicator = project
  .settings(name := "kafka-journal-replicator")
  .dependsOn(journal % "test->test;compile->compile", `eventual-cassandra`)
  .settings(docSettings)
  .settings(libraryDependencies ++= Seq(`cats-helper`, Logback.core % Test, Logback.classic % Test))

lazy val cassandra = project
  .settings(name := "kafka-journal-cassandra")
  .dependsOn(core, `scalatest-io` % Test)
  .settings(docSettings)
  .settings(libraryDependencies ++= Seq(scache, scassandra, `cassandra-sync`))

lazy val `eventual-cassandra` = project
  .settings(name := "kafka-journal-eventual-cassandra")
  .dependsOn(cassandra % "test->test;compile->compile", journal % "test->test;compile->compile")
  .settings(docSettings)
  .settings(libraryDependencies ++= Seq(scassandra))

lazy val `snapshot-cassandra` = project
  .settings(name := "kafka-journal-snapshot-cassandra")
  .dependsOn(cassandra, snapshot % "test->test;compile->compile")
  .settings(docSettings)
  .settings(libraryDependencies ++= Seq(scassandra))

lazy val `journal-circe` = project
  .in(file("circe/core"))
  .dependsOn(journal % "test->test;compile->compile")
  .settings(name := "kafka-journal-circe")
  .settings(docSettings)
  .settings(libraryDependencies ++= Seq(Circe.core, Circe.generic, Circe.jawn))

lazy val `persistence-circe` = project
  .in(file("circe/persistence"))
  .settings(name := "kafka-journal-persistence-circe")
  .settings(docSettings)
  .dependsOn(`journal-circe`, persistence % "test->test;compile->compile")

lazy val `tests` = project
  .settings(name := "kafka-journal-tests")
  .settings(
    Seq(
      publish / skip := true,
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
