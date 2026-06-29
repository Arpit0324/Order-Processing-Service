package com.ops.inventory

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import com.ops.inventory.api.InventoryController
import com.ops.inventory.kafka.InventoryEventProducer
import com.ops.inventory.repository.{InventoryCache, InventoryRepositoryImpl}
import com.ops.inventory.service.InventoryServiceImpl
import com.ops.inventory.stream.InventoryStream
import com.typesafe.config.ConfigFactory
import io.lettuce.core.RedisClient
import org.flywaydb.core.Flyway
import org.slf4j.LoggerFactory
import slick.jdbc.PostgresProfile.api.*
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext

object InventoryApp extends App {

  private val log    = LoggerFactory.getLogger(getClass.getName)
  private val config = ConfigFactory.load()

  // ── Step 1: Flyway migrations ─────────────────────────────────────────────
  log.info("Running Flyway DB migrations...")
  val flyway = Flyway.configure()
    .dataSource(
      config.getString("db.url"),
      config.getString("db.user"),
      config.getString("db.password")
    )
    .locations("classpath:db/migration")
    .baselineOnMigrate(true)
    .validateOnMigrate(true)
    .load()

  val migrationResult = flyway.migrate()
  log.info("Flyway complete: {} migration(s) applied", migrationResult.migrationsExecuted)

  // ── Step 2: Boot ActorSystem ──────────────────────────────────────────────
  given system: ActorSystem[Nothing] =
    ActorSystem(Behaviors.empty, "inventory-service", config)

  given ec: ExecutionContext = system.executionContext

  // ── Step 3: Wire infrastructure ───────────────────────────────────────────
  val db = Database.forURL(
    url      = config.getString("db.url"),
    user     = config.getString("db.user"),
    password = config.getString("db.password"),
    driver   = "org.postgresql.Driver"
  )

  val redisConfig  = config.getConfig("redis")
  val redisClient  = RedisClient.create(s"redis://${redisConfig.getString("host")}:${redisConfig.getInt("port")}")
  val redisConn    = redisClient.connect()
  val redisCommands = redisConn.async()

  // ── Step 4: Wire application layers (bottom-up) ───────────────────────────
  val baseRepo  = InventoryRepositoryImpl(db)
  val cachedRepo = InventoryCache(baseRepo, redisCommands, redisConfig.getLong("ttlSecs"))
  val producer  = InventoryEventProducer()
  val service   = InventoryServiceImpl(cachedRepo, producer)

  // ── Step 5: Start Kafka Streams consumer ──────────────────────────────────
  val stream = InventoryStream(service)
  stream.start()

  // ── Step 6: Start HTTP server ──────────────────────────────────────────────
  val controller = InventoryController(service)
  val host = config.getString("http.host")
  val port = config.getInt("http.port")

  Http().newServerAt(host, port).bind(controller.routes).onComplete {
    case Success(binding) =>
      log.info("Inventory Service started on {}:{}", host, port)
      sys.addShutdownHook {
        log.info("Shutting down Inventory Service...")
        stream.killSwitch.shutdown()
        binding.unbind().onComplete { _ =>
          redisConn.close()
          redisClient.shutdown()
          db.close()
          system.terminate()
        }
      }
    case Failure(ex) =>
      log.error("Failed to bind HTTP server", ex)
      system.terminate()
  }
}
