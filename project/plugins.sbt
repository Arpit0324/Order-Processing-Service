// ── SBT Plugins ──────────────────────────────────────────────────────────────

// Fat JAR packaging: sbt assembly
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.2.0")

// Code formatting: sbt scalafmt / scalafmtCheck
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")

// Dependency updates check: sbt dependencyUpdates
addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.6.4")
