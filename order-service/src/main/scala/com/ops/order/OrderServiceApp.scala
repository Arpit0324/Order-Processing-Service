package com.ops.order

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import com.ops.order.actor.OrderSupervisor
import com.ops.order.api.OrderController
import com.ops.order.infra.{DatabaseConfig, FlywayMigration}
import com.ops.order.kafka.OrderEventProducer
import com.ops.order.repository.OrderRepositoryImpl
import com.ops.order.service.OrderServiceImpl
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

object OrderServiceApp extends App {

  private val log    = LoggerFactory.getLogger(getClass.getName)
  private val config = ConfigFactory.load()

  // ── Step 1: Run Flyway migrations BEFORE anything else ───────────────────────
  log.info("Running Flyway DB migrations...")
  FlywayMigration.run(config)

  // ── Step 2: Boot ActorSystem (Akka Persistence uses the migrated schema) ─────
  given system: ActorSystem[Nothing] =
    ActorSystem(Behaviors.empty, "order-service", config)

  given ec: ExecutionContext = system.executionContext

  // ── Step 3: Wire dependencies (bottom-up: infra → repo → service → api) ─────
  val db          = DatabaseConfig.createDatabase(config)
  val repository  = OrderRepositoryImpl(db)
  val supervisor  = system.systemActorOf(OrderSupervisor(), "order-supervisor")
  val producer    = OrderEventProducer()
  val service     = OrderServiceImpl(supervisor, repository, producer)
  val controller  = OrderController(service)

  // ── Step 4: Start HTTP server ─────────────────────────────────────────────────
  val host = config.getString("http.host")
  val port = config.getInt("http.port")

  Http().newServerAt(host, port).bind(controller.routes).onComplete {
    case Success(binding) =>
      log.info("Order Service started on {}:{}", host, port)
      sys.addShutdownHook {
        log.info("Shutting down Order Service...")
        binding.unbind().onComplete(_ => system.terminate())
        db.close()
      }
    case Failure(ex) =>
      log.error("Failed to bind HTTP server", ex)
      system.terminate()
  }
}
