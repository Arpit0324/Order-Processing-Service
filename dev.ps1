<#
.SYNOPSIS
  Centralized build & dev orchestration for Order Processing System (Windows)

.DESCRIPTION
  Single entry point to compile, test, package, and run all services.
  Wraps SBT (Scala) + Maven (Java) + Docker Compose.

.EXAMPLE
  .\dev.ps1 build          # compile all
  .\dev.ps1 test           # run all tests
  .\dev.ps1 package        # fat JARs
  .\dev.ps1 docker-build   # build Docker images
  .\dev.ps1 up             # full stack in Docker
  .\dev.ps1 up-infra       # infra only (run services from IDE)
  .\dev.ps1 down           # stop containers
  .\dev.ps1 down-v         # stop + wipe volumes
  .\dev.ps1 logs           # tail all logs
  .\dev.ps1 logs order-service  # tail one service
  .\dev.ps1 status         # show containers
  .\dev.ps1 run-order      # run order-service locally
  .\dev.ps1 run-inventory  # run inventory-service locally
  .\dev.ps1 run-notification  # run notification-service locally
  .\dev.ps1 run-gateway    # run api-gateway locally
  .\dev.ps1 clean          # clean all artifacts
#>

param(
    [Parameter(Position = 0)]
    [string]$Command = "help",

    [Parameter(Position = 1)]
    [string]$Service = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ── Helpers ───────────────────────────────────────────────────────────────────

function Write-Header([string]$msg) {
    Write-Host "`n==> $msg" -ForegroundColor Cyan
}

function Invoke-Step([string]$label, [scriptblock]$block) {
    Write-Header $label
    & $block
    if ($LASTEXITCODE -and $LASTEXITCODE -ne 0) {
        Write-Host "FAILED: $label (exit $LASTEXITCODE)" -ForegroundColor Red
        exit $LASTEXITCODE
    }
}

function Assert-Tool([string]$name) {
    if (-not (Get-Command $name -ErrorAction SilentlyContinue)) {
        Write-Host "ERROR: '$name' not found on PATH. Please install it." -ForegroundColor Red
        exit 1
    }
}

# ── Command implementations ───────────────────────────────────────────────────

function Build {
    Assert-Tool "sbt"; Assert-Tool "mvn"
    Invoke-Step "Compiling Scala services (shared + order + inventory)" { sbt compile }
    Invoke-Step "Compiling Java services (api-gateway + notification)"  { mvn compile }
}

function Test {
    Assert-Tool "sbt"; Assert-Tool "mvn"
    Invoke-Step "Testing Scala services" { sbt test }
    Invoke-Step "Testing Java services"  { mvn test }
}

function Package {
    Assert-Tool "sbt"; Assert-Tool "mvn"
    Invoke-Step "Packaging Scala fat JARs (sbt-assembly)" { sbt assembly }
    Invoke-Step "Packaging Java fat JARs (spring-boot)"   { mvn package -DskipTests }
}

function DockerBuild {
    Package
    Invoke-Step "Building Docker images" { docker compose build }
}

function Up {
    Assert-Tool "docker"
    Invoke-Step "Starting full stack" { docker compose up -d }
    Write-Host "`n  Services:" -ForegroundColor Green
    Write-Host "    API Gateway       → http://localhost:8080/swagger-ui.html"
    Write-Host "    Order Service     → http://localhost:8081"
    Write-Host "    Inventory Service → http://localhost:8082"
    Write-Host "    Notification Svc  → http://localhost:8083"
    Write-Host "    Kafka UI          → http://localhost:8090  (admin/admin)"
}

function UpInfra {
    Assert-Tool "docker"
    Invoke-Step "Starting infrastructure only" {
        docker compose up -d postgres redis kafka kafka-init kafka-ui
    }
    Write-Host ""
    Write-Host "  Infrastructure ready. Connect your services to:" -ForegroundColor Green
    Write-Host "    Postgres  → localhost:5432  (user: ops  pass: changeme)"
    Write-Host "    Redis     → localhost:6379"
    Write-Host "    Kafka     → localhost:9092"
    Write-Host "    Kafka UI  → http://localhost:8090  (admin/admin)"
    Write-Host ""
    Write-Host "  Now run services locally with:" -ForegroundColor Yellow
    Write-Host "    .\dev.ps1 run-order"
    Write-Host "    .\dev.ps1 run-inventory"
    Write-Host "    .\dev.ps1 run-notification"
    Write-Host "    .\dev.ps1 run-gateway"
}

function Down([bool]$wipeVolumes = $false) {
    Assert-Tool "docker"
    if ($wipeVolumes) {
        Invoke-Step "Stopping containers and wiping volumes" { docker compose down -v }
    } else {
        Invoke-Step "Stopping containers" { docker compose down }
    }
}

function Logs([string]$svc) {
    Assert-Tool "docker"
    if ($svc) {
        docker compose logs -f $svc
    } else {
        docker compose logs -f
    }
}

function Status {
    Assert-Tool "docker"
    docker compose ps
}

function RunOrder {
    Assert-Tool "sbt"
    Write-Header "Starting order-service locally (port 8081)"
    Write-Host "  Make sure infra is running: .\dev.ps1 up-infra" -ForegroundColor Yellow
    sbt "orderService/run"
}

function RunInventory {
    Assert-Tool "sbt"
    Write-Header "Starting inventory-service locally (port 8082)"
    Write-Host "  Make sure infra is running: .\dev.ps1 up-infra" -ForegroundColor Yellow
    sbt "inventoryService/run"
}

function RunNotification {
    Assert-Tool "mvn"
    Write-Header "Starting notification-service locally (port 8083)"
    Write-Host "  Make sure infra is running: .\dev.ps1 up-infra" -ForegroundColor Yellow
    mvn spring-boot:run -pl notification-service
}

function RunGateway {
    Assert-Tool "mvn"
    Write-Header "Starting api-gateway locally (port 8080)"
    Write-Host "  Make sure infra is running: .\dev.ps1 up-infra" -ForegroundColor Yellow
    mvn spring-boot:run -pl api-gateway
}

function Clean {
    Assert-Tool "sbt"; Assert-Tool "mvn"
    Invoke-Step "Cleaning Scala build artifacts"  { sbt clean }
    Invoke-Step "Cleaning Java build artifacts"   { mvn clean }
}

function Help {
    Write-Host @"

Order Processing System — dev.ps1 commands:

  BUILDING
    build             Compile all services (Scala + Java)
    test              Run all tests
    package           Build fat JARs for all services
    docker-build      Package then build Docker images
    clean             Wipe all build artifacts

  RUNNING (full Docker stack)
    up                Start everything in Docker Compose
    up-infra          Start only infra (Postgres, Redis, Kafka, Kafka-UI)
    down              Stop all containers (keep volumes)
    down-v            Stop all containers AND wipe volumes

  RUNNING (services locally, infra in Docker)
    run-order         sbt orderService/run        (port 8081)
    run-inventory     sbt inventoryService/run     (port 8082)
    run-notification  mvn spring-boot:run          (port 8083)
    run-gateway       mvn spring-boot:run          (port 8080)

  OBSERVABILITY
    logs [service]    Tail logs (all, or named service)
    status            Show running containers and health

  EXAMPLES
    .\dev.ps1 up-infra
    .\dev.ps1 run-order          # in a separate terminal
    .\dev.ps1 logs order-service # in another terminal
"@
}

# ── Dispatch ──────────────────────────────────────────────────────────────────

switch ($Command.ToLower()) {
    "build"             { Build }
    "test"              { Test }
    "package"           { Package }
    "docker-build"      { DockerBuild }
    "up"                { Up }
    "up-infra"          { UpInfra }
    "down"              { Down $false }
    "down-v"            { Down $true }
    "restart"           { docker compose restart }
    "logs"              { Logs $Service }
    "status"            { Status }
    "run-order"         { RunOrder }
    "run-inventory"     { RunInventory }
    "run-notification"  { RunNotification }
    "run-gateway"       { RunGateway }
    "clean"             { Clean }
    "help"              { Help }
    default {
        Write-Host "Unknown command: $Command" -ForegroundColor Red
        Help
        exit 1
    }
}
