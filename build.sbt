// ─────────────────────────────────────────────────────────────────────────────
// Order Processing System — SBT Multi-Project Build
// Manages: shared, order-service, inventory-service
// Java services (api-gateway, notification-service) are managed by pom.xml
// ─────────────────────────────────────────────────────────────────────────────

ThisBuild / scalaVersion     := "3.4.2"
ThisBuild / organization     := "com.ops"
ThisBuild / organizationName := "Order Processing System"
ThisBuild / version          := "1.0.0-SNAPSHOT"

// ── Test reporting ────────────────────────────────────────────────────────────
// Write JUnit XML to target/test-reports/ so GitHub Actions (dorny/test-reporter)
// can parse results and show them inline on pull requests.
ThisBuild / testOptions += Tests.Argument(
  TestFrameworks.ScalaTest,
  "-u", "target/test-reports",   // JUnit XML output directory
  "-o"                            // also print to console
)

// ── Dependency Versions ──────────────────────────────────────────────────────
val pekkoVersion         = "1.1.3"
val pekkoHttpVersion     = "1.1.0"
val pekkoPersistenceJdbc = "1.1.0"
val pekkoKafkaVersion    = "1.1.0"
val slickVersion        = "3.5.1"
val circeVersion        = "0.14.9"
val flywayVersion       = "10.15.0"
val postgresVersion     = "42.7.3"
val logbackVersion      = "1.5.6"
val scalatestVersion    = "3.2.19"
val testcontainersVersion = "0.41.4"

// ── Shared dependencies used across all Scala modules ────────────────────────
val commonDeps = Seq(
  "ch.qos.logback"  % "logback-classic"          % logbackVersion,
  "org.scalatest"  %% "scalatest"                % scalatestVersion % Test
)

// ── shared: Pure domain — Kafka event schemas, value objects, JSON codecs ────
// No framework dependencies. Publishable as a JAR to Maven local for Java services.
lazy val shared = (project in file("shared"))
  .settings(
    name := "shared",
    libraryDependencies ++= commonDeps ++ Seq(
      "io.circe" %% "circe-core"    % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser"  % circeVersion
    )
  )

// ── order-service: Akka Typed + Akka Persistence + Akka HTTP ─────────────────
lazy val orderService = (project in file("order-service"))
  .dependsOn(shared)
  .settings(
    name := "order-service",
    libraryDependencies ++= commonDeps ++ Seq(
      // Pekko
      "org.apache.pekko"   %% "pekko-actor-typed"          % pekkoVersion,
      "org.apache.pekko"   %% "pekko-persistence-typed"    % pekkoVersion,
      "org.apache.pekko"   %% "pekko-persistence-query"    % pekkoVersion,
      "org.apache.pekko"   %% "pekko-http"                 % pekkoHttpVersion,
      "org.apache.pekko"   %% "pekko-http-spray-json"      % pekkoHttpVersion,
      "org.apache.pekko"   %% "pekko-stream"               % pekkoVersion,
      "org.apache.pekko"   %% "pekko-connectors-kafka"     % pekkoKafkaVersion,
      // Persistence
      "org.apache.pekko"   %% "pekko-persistence-jdbc"     % pekkoPersistenceJdbc,
      "com.typesafe.slick" %% "slick"                     % slickVersion,
      "com.typesafe.slick" %% "slick-hikaricp"            % slickVersion,
      "org.postgresql"      % "postgresql"                % postgresVersion,
      // Migrations
      "org.flywaydb"        % "flyway-core"               % flywayVersion,
      "org.flywaydb"        % "flyway-database-postgresql" % flywayVersion,
      // JSON
      "io.circe"           %% "circe-core"                % circeVersion,
      "io.circe"           %% "circe-generic"             % circeVersion,
      "io.circe"           %% "circe-parser"              % circeVersion,
      "com.github.pjfanning" %% "pekko-http-circe"          % "3.0.0",
      // Test
      "org.apache.pekko"   %% "pekko-actor-testkit-typed"  % pekkoVersion       % Test,
      "org.apache.pekko"   %% "pekko-stream-testkit"       % pekkoVersion       % Test,
      "org.apache.pekko"   %% "pekko-http-testkit"         % pekkoHttpVersion   % Test,
      "com.dimafeng"       %% "testcontainers-scala-postgresql" % testcontainersVersion % Test,
      "com.dimafeng"       %% "testcontainers-scala-kafka"      % testcontainersVersion % Test
    ),
    assembly / mainClass := Some("com.ops.order.OrderServiceApp"),
    assembly / assemblyJarName := "order-service.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "MANIFEST.MF")       => MergeStrategy.discard
      case PathList("META-INF", "services", _*)       => MergeStrategy.concat
      case PathList("META-INF", "maven", _*)          => MergeStrategy.discard
      case PathList("reference.conf")                 => MergeStrategy.concat
      case PathList("application.conf")               => MergeStrategy.first
      case _                                          => MergeStrategy.first
    }
  )

// ── inventory-service: Akka Streams + Alpakka Kafka + Redis ──────────────────
lazy val inventoryService = (project in file("inventory-service"))
  .dependsOn(shared)
  .settings(
    name := "inventory-service",
    libraryDependencies ++= commonDeps ++ Seq(
      // Pekko Streams
      "org.apache.pekko"   %% "pekko-actor-typed"          % pekkoVersion,
      "org.apache.pekko"   %% "pekko-stream"               % pekkoVersion,
      "org.apache.pekko"   %% "pekko-http"                 % pekkoHttpVersion,
      "org.apache.pekko"   %% "pekko-http-spray-json"      % pekkoHttpVersion,
      "org.apache.pekko"   %% "pekko-connectors-kafka"     % pekkoKafkaVersion,
      // Persistence
      "com.typesafe.slick" %% "slick"                     % slickVersion,
      "com.typesafe.slick" %% "slick-hikaricp"            % slickVersion,
      "org.postgresql"      % "postgresql"                % postgresVersion,
      // Redis (async Lettuce client)
      "io.lettuce"          % "lettuce-core"              % "6.3.2.RELEASE",
      // Migrations
      "org.flywaydb"        % "flyway-core"               % flywayVersion,
      "org.flywaydb"        % "flyway-database-postgresql" % flywayVersion,
      // JSON
      "io.circe"           %% "circe-core"                % circeVersion,
      "io.circe"           %% "circe-generic"             % circeVersion,
      "io.circe"           %% "circe-parser"              % circeVersion,
      "com.github.pjfanning" %% "pekko-http-circe"          % "3.0.0",
      // Test
      "org.apache.pekko"   %% "pekko-stream-testkit"       % pekkoVersion       % Test,
      "org.apache.pekko"   %% "pekko-http-testkit"         % pekkoHttpVersion   % Test,
      "com.dimafeng"       %% "testcontainers-scala-postgresql" % testcontainersVersion % Test,
      "com.dimafeng"       %% "testcontainers-scala-kafka"      % testcontainersVersion % Test,
      "com.dimafeng"       %% "testcontainers-scala-redis"      % testcontainersVersion % Test
    ),
    assembly / mainClass := Some("com.ops.inventory.InventoryApp"),
    assembly / assemblyJarName := "inventory-service.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "MANIFEST.MF")       => MergeStrategy.discard
      case PathList("META-INF", "services", _*)       => MergeStrategy.concat
      case PathList("reference.conf")                 => MergeStrategy.concat
      case _                                          => MergeStrategy.first
    }
  )

// ── root aggregate: sbt test / sbt assembly runs all Scala modules ───────────
lazy val root = (project in file("."))
  .aggregate(shared, orderService, inventoryService)
  .settings(
    name           := "order-processing-system",
    publish / skip := true  // root is not published
  )
