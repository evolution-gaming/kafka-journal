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
    "-deprecation",                      // Emit warning and location for usages of deprecated APIs.
    "-encoding", "utf-8",                // Specify character encoding used by source files.
    "-explaintypes",                     // Explain type errors in more detail.
    "-feature",                          // Emit warning and location for usages of features that should be imported explicitly.
    "-language:existentials",            // Existential types (besides wildcard types) can be written and inferred
    "-language:experimental.macros",     // Allow macro definition (besides implementation and application)
    "-language:higherKinds",             // Allow higher-kinded types
    "-language:implicitConversions",     // Allow definition of implicit functions called views
    "-unchecked",                        // Enable additional warnings where generated code depends on assumptions.
    "-Xcheckinit",                       // Wrap field accessors to throw an exception on uninitialized access.
//    "-Xfatal-warnings",                  // Fail the compilation if there are any warnings.
    "-Xfuture",                          // Turn on future language features.
    "-Xlint:adapted-args",               // Warn if an argument list is modified to match the receiver.
    "-Xlint:by-name-right-associative",  // By-name parameter of right associative operator.
    "-Xlint:constant",                   // Evaluation of a constant arithmetic expression results in an error.
    "-Xlint:delayedinit-select",         // Selecting member of DelayedInit.
    "-Xlint:doc-detached",               // A Scaladoc comment appears to be detached from its element.
    "-Xlint:inaccessible",               // Warn about inaccessible types in method signatures.
    "-Xlint:infer-any",                  // Warn when a type argument is inferred to be `Any`.
    "-Xlint:missing-interpolator",       // A string literal appears to be missing an interpolator id.
    "-Xlint:nullary-override",           // Warn when non-nullary `def f()' overrides nullary `def f'.
    "-Xlint:nullary-unit",               // Warn when nullary methods return Unit.
    "-Xlint:option-implicit",            // Option.apply used implicit view.
    "-Xlint:package-object-classes",     // Class or object defined in package object.
    "-Xlint:poly-implicit-overload",     // Parameterized overloaded implicit methods are not visible as view bounds.
    "-Xlint:private-shadow",             // A private field (or class parameter) shadows a superclass field.
    "-Xlint:stars-align",                // Pattern sequence wildcard must align with sequence component.
    "-Xlint:type-parameter-shadow",      // A local type parameter shadows a type already in scope.
    "-Xlint:unsound-match",              // Pattern match may not be typesafe.
    "-Yno-adapted-args",                 // Do not adapt an argument list (either by inserting () or creating a tuple) to match the receiver.
    "-Ypartial-unification",             // Enable partial unification in type constructor inference
    "-Ywarn-dead-code",                  // Warn when dead code is identified.
    "-Ywarn-extra-implicit",             // Warn when more than one implicit parameter section is defined.
    "-Ywarn-inaccessible",               // Warn about inaccessible types in method signatures.
    "-Ywarn-infer-any",                  // Warn when a type argument is inferred to be `Any`.
    "-Ywarn-nullary-override",           // Warn when non-nullary `def f()' overrides nullary `def f'.
    "-Ywarn-nullary-unit",               // Warn when nullary methods return Unit.
    "-Ywarn-numeric-widen",              // Warn when numerics are widened.
    "-Ywarn-unused:implicits",           // Warn if an implicit parameter is unused.
    "-Ywarn-unused:imports",             // Warn if an import selector is not referenced.
    "-Ywarn-unused:locals",              // Warn if a local definition is unused.
    "-Ywarn-unused:params",              // Warn if a value parameter is unused.
    "-Ywarn-unused:patvars",             // Warn if a variable bound in a pattern is unused.
    "-Ywarn-unused:privates",            // Warn if a private member is unused.
    "-Ywarn-value-discard"               // Warn when non-Unit expression results are unused.
  ),
  scalacOptions in (Compile, console) --= Seq("-Ywarn-unused:imports", "-Xfatal-warnings"),
  scalacOptions in(Compile, doc) ++= Seq("-groups", "-implicits", "-no-link-warnings"),
  resolvers += Resolver.bintrayRepo("evolutiongaming", "maven"),
  licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT"))),
  releaseCrossBuild := true)


lazy val root = (project in file(".")
  settings (name := "kafka-journal")
  settings commonSettings
  settings (skip in publish := true)
  aggregate(
    rng,
    `scalatest-io`,
    `cats-effect-helpers`,
    cache,
    stream,
    retry,
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
    Cats.core)))

lazy val retry = (project in file("retry")
  settings (name := "kafka-journal-retry")
  settings commonSettings
  dependsOn (rng, `cats-effect-helpers`)
  settings (libraryDependencies ++= Seq(
    scalatest % Test,
    Cats.core)))

lazy val rng = (project in file("rng")
  settings (name := "kafka-journal-rng")
  settings commonSettings
  dependsOn `cats-effect-helpers`
  settings (libraryDependencies ++= Seq(
    scalatest % Test,
    Cats.core,
    Cats.effect)))

lazy val `cats-effect-helpers` = (project in file("cats-effect-helpers")
  settings (name := "kafka-journal-cats-effect-helpers")
  settings commonSettings
  dependsOn `scalatest-io` % "test->compile"
  settings (libraryDependencies ++= Seq(
    scalatest % Test,
    Cats.core,
    Cats.effect)))

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
  dependsOn (stream, retry, cache, `scalatest-io` % "test->compile")
  settings (libraryDependencies ++= Seq(
    Akka.actor,
    Akka.stream,
    Akka.testkit % Test,
    Akka.slf4j % Test,
    Kafka.`kafka-clients`,
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
  dependsOn (
    journal % "test->test;compile->compile", 
    `eventual-cassandra`)
  settings (libraryDependencies ++= Seq(
    `akka-serialization`,
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