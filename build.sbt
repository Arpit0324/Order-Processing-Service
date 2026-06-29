// ─────────────────────────────────────────────────────────────────────────────
// Order Processing System — SBT Multi-Project Build
// Manages: shared, order-service, inventory-service
// Java services (api-gateway, notification-service) are managed by pom.xml
// ─────────────────────────────────────────────────────────────────────────────

ThisBuild / scalaVersion     := "3.4.2"
ThisBuild / organization     := "com.ops"
ThisBuild / organizationName := "Order Processing System"
ThisBuild / version          := "1.0.0-SNAPSHOT"

// ── Dependency Versions ──────────────────────────────────────────────────────
val akkaVersion         = "2.9.3"
val akkaHttpVersion     = "10.6.3"
val akkaPersistenceJdbc = "5.4.1"
val alpakkaKafkaVersion = "5.0.0"
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
      // Akka
      "com.typesafe.akka"  %% "akka-actor-typed"          % akkaVersion,
      "com.typesafe.akka"  %% "akka-persistence-typed"    % akkaVersion,
      "com.typesafe.akka"  %% "akka-persistence-query"    % akkaVersion,
      "com.typesafe.akka"  %% "akka-http"                 % akkaHttpVersion,
      "com.typesafe.akka"  %% "akka-http-spray-json"      % akkaHttpVersion,
      "com.typesafe.akka"  %% "akka-stream"               % akkaVersion,
      "com.typesafe.akka"  %% "akka-stream-kafka"         % alpakkaKafkaVersion,
      // Persistence
      "com.lightbend.akka" %% "akka-persistence-jdbc"     % akkaPersistenceJdbc,
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
      "de.heikoseeberger"  %% "akka-http-circe"           % "1.39.2",
      // Test
      "com.typesafe.akka"  %% "akka-actor-testkit-typed"  % akkaVersion        % Test,
      "com.typesafe.akka"  %% "akka-stream-testkit"       % akkaVersion        % Test,
      "com.typesafe.akka"  %% "akka-http-testkit"         % akkaHttpVersion    % Test,
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
      // Akka Streams
      "com.typesafe.akka"  %% "akka-stream"               % akkaVersion,
      "com.typesafe.akka"  %% "akka-http"                 % akkaHttpVersion,
      "com.typesafe.akka"  %% "akka-http-spray-json"      % akkaHttpVersion,
      "com.typesafe.akka"  %% "akka-stream-kafka"         % alpakkaKafkaVersion,
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
      "de.heikoseeberger"  %% "akka-http-circe"           % "1.39.2",
      // Test
      "com.typesafe.akka"  %% "akka-stream-testkit"       % akkaVersion        % Test,
      "com.typesafe.akka"  %% "akka-http-testkit"         % akkaHttpVersion    % Test,
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
