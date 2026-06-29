package com.ops.order.infra

import com.typesafe.config.Config
import org.flywaydb.core.Flyway
import org.slf4j.LoggerFactory

// ── FlywayMigration — runs BEFORE ActorSystem starts ─────────────────────────
// Ensures DB schema is up-to-date. On first run: creates all tables.
// On subsequent runs: applies only new migrations.
object FlywayMigration {

  private val log = LoggerFactory.getLogger(getClass.getName)

  def run(config: Config): Unit = {
    val dbConfig = config.getConfig("db")
    val flyway = Flyway.configure()
      .dataSource(
        dbConfig.getString("url"),
        dbConfig.getString("user"),
        dbConfig.getString("password")
      )
      .locations("classpath:db/migration")
      .baselineOnMigrate(true)   // safe for first-time deploy on existing DB
      .validateOnMigrate(true)   // fail fast if migration files were tampered with
      .outOfOrder(false)         // strict ordering: no gaps allowed
      .load()

    val result = flyway.migrate()
    log.info("Flyway migration complete: {} migration(s) applied", result.migrationsExecuted)
  }
}
