externalResolvers += Resolver.bintrayIvyRepo("evolutiongaming", "sbt-plugins")

addSbtPlugin("org.foundweekends" % "sbt-bintray" % "0.5.6")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.6.1")

addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.2.7")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.13")

addSbtPlugin("com.evolutiongaming" % "sbt-scalac-opts-plugin" % "0.0.4")