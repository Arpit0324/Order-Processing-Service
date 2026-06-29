package com.ops.order.infra

import com.typesafe.config.Config
import com.zaxxer.hikari.HikariConfig
import slick.jdbc.PostgresProfile.api.*

// ── DatabaseConfig — creates the Slick Database with HikariCP pool ────────────
object DatabaseConfig {

  def createDatabase(config: Config): Database = {
    val dbConfig = config.getConfig("db")
    Database.forURL(
      url      = dbConfig.getString("url"),
      user     = dbConfig.getString("user"),
      password = dbConfig.getString("password"),
      driver   = "org.postgresql.Driver",
      prop     = hikariProperties(dbConfig)
    )
  }

  private def hikariProperties(dbConfig: Config): java.util.Properties = {
    val props = new java.util.Properties()
    val hikari = dbConfig.getConfig("hikari")
    props.setProperty("maximumPoolSize",   hikari.getString("maximumPoolSize"))
    props.setProperty("minimumIdle",       hikari.getString("minimumIdle"))
    props.setProperty("connectionTimeout", hikari.getString("connectionTimeout"))
    props.setProperty("idleTimeout",       hikari.getString("idleTimeout"))
    props.setProperty("maxLifetime",       hikari.getString("maxLifetime"))
    props
  }
}
