import Dependencies.*
import com.typesafe.tools.mima.core.*
import sbt.Package.ManifestAttributes

lazy val commonSettings = Seq(
  organization := "com.evolution",
  organizationName := "Evolution",
  organizationHomepage := Some(uri("https://evolution.com")),
  homepage := Some(uri("https://github.com/evolution-gaming/kafka-journal")),
  startYear := Some(2018),
  crossScalaVersions := Seq("2.13.18", "3.3.8"),
  scalaVersion := crossScalaVersions.value.head,
  scalacOptions ++= Seq(
    "-release:17",
    "-deprecation",
  ),
  scalacOptions ++= crossSettings(
    scalaVersion = scalaVersion.value,
    // Good compiler options for Scala 2.13 are coming from com.evolution:sbt-scalac-opts-plugin:0.1.0,
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
  // Under sbt 2 the compile cache can skip re-creating scoverage's data directory after `clean`.
  // When a module's instrumented code runs inside another module's forked test JVM (before that
  // module's own tests, if any, have run), scoverage.Invoker then crashes writing measurements to
  // the missing directory. Ensure it always exists after compile whenever coverage is enabled.
  Compile / compile := Def.uncached {
    val compiled = (Compile / compile).value
    if (coverageEnabled.value) {
      val _ = (crossTarget.value / "scoverage-data").mkdirs()
    }
    compiled
  },
  publishTo := Some(Resolver.evolutionReleases),
  licenses := Seq(("MIT", uri("https://opensource.org/licenses/MIT"))),
  // set up compiler plugins:
  libraryDependencies ++= crossSettings(
    scalaVersion = scalaVersion.value,
    if3 = Seq(),
    if2 = Seq(compilerPlugin(KindProjector.cross(CrossVersion.full))),
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
  versionPolicyIgnored ++= Seq(
    // add libraries here that are known to be binary compatible, like:
//    "com.evolutiongaming" %% "smetrics",
  ),
)

ThisBuild / mimaBinaryIssueFilters ++= Seq(
  // add mima check exceptions here, like:
//  ProblemFilters.exclude[IncompatibleMethTypeProblem](
//    "com.evolution.kafka.journal.replicator.TopicReplicator#ConsumerOf.make",
//  ),
  // journal fork detection counter added to the metrics SPI, which has no implementors outside of
  // `ReplicatedJournal.Metrics.const` / `.make`
  ProblemFilters.exclude[ReversedMissingMethodProblem](
    "com.evolution.kafka.journal.eventual.ReplicatedJournal#Metrics.journalForkDetected",
  ),
)

ThisBuild / libraryDependencySchemes ++= Seq(
  // add mima check overrides for RC-level libraries, like:
//  "org.tpolecat" %% "doobie-core" % VersionScheme.Always,
)

val alias: Seq[sbt.Def.Setting[?]] =
  addCommandAlias("fmt", "+all scalafmtRepo") ++
    addCommandAlias(
      "check", // check is called with + from the release action
      "all versionPolicyCheck Compile/doc scalafmtCheckRepo",
    ) ++
    addCommandAlias("build", "all compile testFull")

lazy val root = project
  .in(file("."))
  .settings(name := "kafka-journal")
  .settings(normalizedName := "root")
  .settings(commonSettings)
  .settings(publish / skip := true)
  .settings(alias)
  .aggregate(
    core,
    journal,
    snapshot,
    replicator,
    cassandra,
    eventualCassandra,
    snapshotCassandra,
    circe,
    akkaPersistence,
    akkaPersistenceCirce,
    akkaIntegrationTests,
    pekkoPersistence,
    pekkoPersistenceCirce,
    pekkoIntegrationTests,
    scalaTestIO,
    demo,
  )

lazy val core = project
  .in(file("core"))
  .settings(name := "kafka-journal-core")
  .settings(commonSettings)
  .dependsOn(scalaTestIO % Test)
  .settings(
    libraryDependencies ++= Seq(
      SKafka,
      CatsHelper,
      PlayJson,
      PlayJsonJsoniter,
      SStream,
      Hostname,
      Pureconfig.Core,
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
  .dependsOn(core % "test->test;compile->compile", scalaTestIO % Test)
  .settings(
    libraryDependencies ++= Seq(
      KafkaClients,
      SKafka,
      Random,
      Retry,
      CatsHelper,
      PlayJson,
      PlayJsonJsoniter,
      SCache,
      Pureconfig.Core,
      Pureconfig.Cats,
      Smetrics.SMetrics,
      SStream,
      Cats.Core,
      Cats.Effect,
      ResourcePool,
      ScalaTest % Test,
      ExecutorTools % Test,
      Logback.Core % Test,
      Logback.Classic % Test,
    ),
    libraryDependencies ++= crossSettings(
      scalaVersion = scalaVersion.value,
      if2 = Seq(Pureconfig.Scala2.Generic),
      if3 = Seq(Pureconfig.Scala3.Generic),
    ),
  )

lazy val snapshot = project
  .in(file("snapshot"))
  .settings(name := "kafka-journal-snapshot")
  .settings(commonSettings)
  .dependsOn(core)
  .settings(libraryDependencies ++= Seq(ScalaTest % Test))

lazy val akkaPersistence = project
  .in(file("akka/persistence"))
  .settings(name := "kafka-journal-akka-persistence")
  .settings(commonSettings)
  .dependsOn(journal % "test->test;compile->compile", eventualCassandra, snapshotCassandra)
  .settings(
    libraryDependencies ++= Seq(
      AkkaSerialization,
      CatsHelper,
      Akka.Persistence,
      Akka.Testkit % Test,
      AkkaTestActor % Test,
    ),
  )

lazy val pekkoPersistence = project
  .in(file("pekko/persistence"))
  .settings(name := "kafka-journal-pekko-persistence")
  .settings(commonSettings)
  .dependsOn(journal % "test->test;compile->compile", eventualCassandra, snapshotCassandra)
  .settings(
    libraryDependencies ++= Seq(
      PekkoSerialization,
      CatsHelper,
      Pekko.Persistence,
      Pekko.Testkit % Test,
      PekkoTestActor % Test,
    ),
  )

lazy val akkaIntegrationTests = project
  .in(file("akka/integration-tests"))
  .configure(asIntegrationTestModule)
  .settings(name := "kafka-journal-akka-integration-tests")
  .dependsOn(akkaPersistence % "test->test;compile->compile", akkaPersistenceCirce, replicator)
  .settings(
    libraryDependencies ++= Seq(
      Akka.Slf4j % Test,
      Akka.PersistenceTck % Test,
    ),
  )

lazy val pekkoIntegrationTests = project
  .in(file("pekko/integration-tests"))
  .configure(asIntegrationTestModule)
  .settings(name := "kafka-journal-pekko-integration-tests")
  .dependsOn(pekkoPersistence % "test->test;compile->compile", pekkoPersistenceCirce, replicator)
  .settings(
    libraryDependencies ++= Seq(
      Pekko.Slf4j % Test,
      Pekko.PersistenceTck % Test,
    ),
  )

lazy val replicator = project
  .in(file("replicator"))
  .settings(name := "kafka-journal-replicator")
  .settings(commonSettings)
  .dependsOn(
    journal % "test->test",
    eventualCassandra,
    scalaTestIO % Test,
  )
  .settings(libraryDependencies ++= Seq(
    CatsHelper,
    Logback.Core % Test,
    Logback.Classic % Test,
    ScalaTest % Test,
  ))

lazy val cassandra = project
  .in(file("cassandra"))
  .settings(name := "kafka-journal-cassandra")
  .settings(commonSettings)
  .dependsOn(core, scalaTestIO % Test)
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

lazy val eventualCassandra = project
  .in(file("eventual-cassandra"))
  .settings(name := "kafka-journal-eventual-cassandra")
  .settings(commonSettings)
  .dependsOn(cassandra % "test->test;compile->compile", journal % "test->test;compile->compile")

lazy val snapshotCassandra = project
  .in(file("snapshot-cassandra"))
  .settings(name := "kafka-journal-snapshot-cassandra")
  .settings(commonSettings)
  .dependsOn(cassandra, snapshot % "test->test;compile->compile")

lazy val circe = project
  .in(file("circe"))
  .settings(name := "kafka-journal-circe")
  .settings(commonSettings)
  .dependsOn(journal % "test->test;compile->compile")
  .settings(libraryDependencies ++= Seq(Circe.Core, Circe.Generic, Circe.Jawn))

lazy val akkaPersistenceCirce = project
  .in(file("akka/persistence-circe"))
  .settings(name := "kafka-journal-akka-persistence-circe")
  .settings(commonSettings)
  .dependsOn(circe, akkaPersistence % "test->test;compile->compile")

lazy val pekkoPersistenceCirce = project
  .in(file("pekko/persistence-circe"))
  .settings(name := "kafka-journal-pekko-persistence-circe")
  .settings(commonSettings)
  .dependsOn(circe, pekkoPersistence % "test->test;compile->compile")

lazy val scalaTestIO = project
  .in(file("scalatest-io"))
  .settings(name := "kafka-journal-scalatest-io")
  .settings(commonSettings)
  .settings(publish / skip := true)
  .settings(libraryDependencies ++= Seq(ScalaTest, Smetrics.SMetrics, CatsHelper, Cats.Core, Cats.Effect))

/**
 * A resting ground for sample demo apps which used to reside alongside integration tests
 */
lazy val demo = project
  .in(file("demo"))
  .settings(name := "kafka-journal-demo")
  .settings(commonSettings)
  .settings(
    Seq(
      publish / skip := true,
    ),
  )
  .dependsOn(
    journal,
    eventualCassandra,
    replicator,
  )
  .settings(libraryDependencies ++= Seq(
    Logback.Core,
    Logback.Classic,
  ))

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

/**
 * Configures submodule for running integration tests
 *
 * Parallel test execution is disabled on both individual test suite level and between integration
 * test modules (with `fork := true`), so the tests do not interfere with each other.
 *
 * The forking is also enabled to ensure the correct cleanup of statically allocated resources, like
 * test containers.
 */
def asIntegrationTestModule(p: Project): Project = {
  p
    .settings(commonSettings)
    .settings(
      publish / skip := true,
      Test / fork := true,
      Test / parallelExecution := false,
      Test / javaOptions ++= Seq("-Xms3G", "-Xmx3G"),

      libraryDependencies ++= Seq(
        ScalaTest % Test,
        TestContainers.Cassandra % Test,
        TestContainers.Kafka % Test,
        Slf4j.Api % Test,
        Slf4j.Log4jOverSlf4j % Test,
        Logback.Core % Test,
        Logback.Classic % Test,
      ),
    )
}
