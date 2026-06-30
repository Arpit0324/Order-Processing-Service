# =============================================================================
# Order Processing System — Centralized Build Orchestration
# =============================================================================
# Requires: Java 21, SBT 1.x, Maven 3.9+, Docker, Docker Compose v2
#
# Usage:
#   make build          → compile all (Scala + Java)
#   make test           → run all tests
#   make package        → fat JARs for all services
#   make docker-build   → build all Docker images
#   make up             → start full stack (infra + all services in Docker)
#   make up-infra       → start only infra (run services locally via IDE/sbt/mvn)
#   make down           → stop everything
#   make clean          → wipe all build artifacts
#   make logs           → tail all container logs
#   make logs s=<svc>   → tail a single service  (e.g. make logs s=order-service)
#   make status         → show running containers
#   make help           → this message
# =============================================================================

.DEFAULT_GOAL := help
COMPOSE       := docker compose
SBT           := sbt
MVN           := mvn

# Allow overriding the service for `make logs s=order-service`
s ?=

# ── Compile ──────────────────────────────────────────────────────────────────

.PHONY: build build-scala build-java
build: build-scala build-java   ## Compile all services

build-scala:                    ## Compile Scala services (shared + order + inventory)
	$(SBT) compile

build-java:                     ## Compile Java services (api-gateway + notification)
	$(MVN) compile

# ── Test ─────────────────────────────────────────────────────────────────────

.PHONY: test test-scala test-java
test: test-scala test-java      ## Run all tests

test-scala:                     ## Run Scala tests (requires Docker for testcontainers)
	$(SBT) test

test-java:                      ## Run Java tests (requires Docker for testcontainers)
	$(MVN) test

# ── Package ──────────────────────────────────────────────────────────────────

.PHONY: package package-scala package-java
package: package-scala package-java   ## Build fat JARs for all services

package-scala:                  ## sbt-assembly fat JARs
	$(SBT) assembly

package-java:                   ## Maven fat JARs (spring-boot:repackage)
	$(MVN) package -DskipTests

# ── Docker ───────────────────────────────────────────────────────────────────

.PHONY: docker-build up up-infra down down-v restart logs status
docker-build: package           ## Build all Docker images (packages first)
	$(COMPOSE) build

up:                             ## Start full stack (infra + all services)
	$(COMPOSE) up -d

up-infra:                       ## Start only infra (Postgres, Redis, Kafka, Kafka-UI)
	$(COMPOSE) up -d postgres redis kafka kafka-init kafka-ui
	@echo ""
	@echo "  Infrastructure ready. Connect your services to:"
	@echo "    Postgres  → localhost:5432  (user: ops  pass: changeme)"
	@echo "    Redis     → localhost:6379"
	@echo "    Kafka     → localhost:9092"
	@echo "    Kafka UI  → http://localhost:8090  (admin/admin)"
	@echo ""
	@echo "  Run services locally:"
	@echo "    sbt 'orderService/run'       (port 8081)"
	@echo "    sbt 'inventoryService/run'   (port 8082)"
	@echo "    mvn spring-boot:run -pl notification-service  (port 8083)"
	@echo "    mvn spring-boot:run -pl api-gateway           (port 8080)"

down:                           ## Stop all containers (keep volumes)
	$(COMPOSE) down

down-v:                         ## Stop all containers AND wipe volumes (fresh start)
	$(COMPOSE) down -v

restart:                        ## Restart all containers
	$(COMPOSE) restart

logs:                           ## Tail logs. Use s=<service> for a single service
ifdef s
	$(COMPOSE) logs -f $(s)
else
	$(COMPOSE) logs -f
endif

status:                         ## Show running containers and health
	$(COMPOSE) ps

# ── Individual service run (local, no Docker) ────────────────────────────────
# These targets assume `make up-infra` has already been run.

.PHONY: run-order run-inventory run-notification run-gateway
run-order:                      ## Run order-service locally (JVM, no Docker)
	$(SBT) "orderService/run"

run-inventory:                  ## Run inventory-service locally (JVM, no Docker)
	$(SBT) "inventoryService/run"

run-notification:               ## Run notification-service locally (JVM, no Docker)
	$(MVN) spring-boot:run -pl notification-service

run-gateway:                    ## Run api-gateway locally (JVM, no Docker)
	$(MVN) spring-boot:run -pl api-gateway

# ── Clean ────────────────────────────────────────────────────────────────────

.PHONY: clean clean-scala clean-java clean-docker
clean: clean-scala clean-java   ## Clean all build artifacts

clean-scala:
	$(SBT) clean

clean-java:
	$(MVN) clean

clean-docker:                   ## Remove stopped containers and dangling images
	docker system prune -f

# ── Help ─────────────────────────────────────────────────────────────────────

.PHONY: help
help:
	@echo ""
	@echo "Order Processing System — available targets:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*##"}; {printf "  %-20s %s\n", $$1, $$2}'
	@echo ""
