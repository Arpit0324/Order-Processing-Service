# Running the Order Processing System — End-to-End Guide

## Prerequisites

| Tool | Version | Check |
|------|---------|-------|
| Java | 21+ | `java -version` |
| SBT | 1.x | `sbt --version` |
| Maven | 3.9+ | `mvn --version` |
| Docker + Docker Compose v2 | latest | `docker compose version` |

---

## One-Time Setup — JWT Key Pair

The API Gateway verifies RS256 JWTs using a public key supplied at runtime via the
`JWT_PUBLIC_KEY` environment variable. **Keys are never committed to the repo.**

### 1. Generate the RSA-2048 key pair

**Windows (PowerShell — no extra tools needed, uses JDK 21's `jshell`)**
```powershell
@'
import java.security.*; import java.util.Base64; import java.nio.file.*;
KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA"); gen.initialize(2048);
KeyPair pair = gen.generateKeyPair();
var enc = Base64.getMimeEncoder(64, new byte[]{'\n'});
Files.writeString(Path.of("private.pem"), "-----BEGIN PRIVATE KEY-----\n" + enc.encodeToString(pair.getPrivate().getEncoded()) + "\n-----END PRIVATE KEY-----\n");
Files.writeString(Path.of("public.pem"),  "-----BEGIN PUBLIC KEY-----\n"  + enc.encodeToString(pair.getPublic().getEncoded())  + "\n-----END PUBLIC KEY-----\n");
System.out.println("JWT_PUBLIC_KEY=" + Base64.getEncoder().encodeToString(pair.getPublic().getEncoded()));
/exit
'@ | jshell --execution local -
```

**Linux / Mac (requires `openssl`)**
```bash
openssl genrsa -out private.pem 2048
openssl rsa -in private.pem -pubout -out public.pem
```

> Keep `private.pem` safe — it signs tokens. Both files are gitignored (`*.pem`).

### 2. Extract the public key value for `JWT_PUBLIC_KEY`

The env var expects the PEM body — the base64 content **without** the `-----BEGIN/END-----`
header lines and **without** newlines.

> **Windows shortcut**: the `jshell` command above already prints `JWT_PUBLIC_KEY=<value>` — copy that line's value directly into `.env`.

**Linux / Mac**
```bash
grep -v '^-----' public.pem | tr -d '\n'
```

**Windows (PowerShell — from existing `public.pem`)**
```powershell
(Get-Content public.pem | Where-Object { $_ -notmatch '^-----' }) -join ''
```

Copy the output — it will look like `MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...`

### 3. Create your `.env` file

```bash
cp .env.example .env          # Linux/Mac
Copy-Item .env.example .env   # Windows PowerShell
```

Open `.env` and replace `<paste-base64-public-key-here>` with the value from step 2:

```
JWT_PUBLIC_KEY=MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...
```

> `.env` is gitignored. **Never commit it.**

---

## Option A — Full Docker Stack (Recommended)

Everything runs inside Docker. Simplest way to get the whole system up.

### 1. Build fat JARs and Docker images

**Linux/Mac**
```bash
make package
make docker-build
```

**Windows**
```powershell
.\dev.ps1 package
.\dev.ps1 docker-build
```

### 2. Start the full stack

**Linux/Mac**
```bash
make up
```

**Windows**
```powershell
.\dev.ps1 up
```

This starts 9 containers:
- `postgres` — 3 databases (`ops_orders`, `ops_inventory`, `ops_notifications`)
- `redis` — cache for inventory and rate limiting
- `kafka` — KRaft mode (no ZooKeeper)
- `kafka-init` — one-shot container that creates all 9 Kafka topics, then exits
- `kafka-ui` — Kafka browser UI
- `order-service` — Scala/Pekko (port 8081)
- `inventory-service` — Scala/Pekko Streams (port 8082)
- `notification-service` — Java/Spring Boot (port 8083)
- `api-gateway` — Spring Cloud Gateway (port 8080)

### 3. Verify everything is healthy

```bash
docker compose ps
```

All containers except `kafka-init` should show `running` or `healthy`.

### 4. Tail logs

```bash
# All services
make logs                        # Linux/Mac
.\dev.ps1 logs                   # Windows

# Single service
make logs s=order-service        # Linux/Mac
.\dev.ps1 logs order-service     # Windows
```

### 5. Stop

```bash
make down           # keep volumes (data survives)
make down-v         # wipe volumes (fresh start next time)
```

---

## Option B — Infrastructure in Docker, Services Locally

Run Postgres/Redis/Kafka in Docker but each microservice on your JVM directly.
Useful for debugging or fast iteration.

### 1. Start only infrastructure

**Linux/Mac**
```bash
make up-infra
```

**Windows**
```powershell
.\dev.ps1 up-infra
```

Exposed ports after this step:

| Service | Local Port |
|---------|-----------|
| Postgres | `localhost:5432` (user: `ops`, pass: `changeme`) |
| Redis | `localhost:6379` |
| Kafka | `localhost:9092` |
| Kafka UI | http://localhost:8090 (admin / admin) |

### 2. Run each service (open 4 terminals)

**Terminal 1 — Order Service (Scala, port 8081)**
```bash
sbt "orderService/run"
```

**Terminal 2 — Inventory Service (Scala, port 8082)**
```bash
sbt "inventoryService/run"
```

**Terminal 3 — Notification Service (Java/Spring Boot, port 8083)**
```bash
mvn spring-boot:run -pl notification-service
```

**Terminal 4 — API Gateway (Java/Spring Boot, port 8080)**
```bash
mvn spring-boot:run -pl api-gateway
```

> Start order-service and inventory-service before api-gateway, so circuit breakers
> don't open on first health check.

---

## Service URLs

| Service | URL |
|---------|-----|
| API Gateway (entry point) | http://localhost:8080 |
| Swagger / API Docs | http://localhost:8080/swagger-ui.html |
| Gateway Health | http://localhost:8080/actuator/health |
| Order Service (direct) | http://localhost:8081 |
| Inventory Service (direct) | http://localhost:8082 |
| Notification Service (direct) | http://localhost:8083 |
| Kafka UI | http://localhost:8090 |

---

## Quick Smoke Test (end-to-end flow)

All requests go through the API Gateway on port **8080**.

### 1. Check gateway health
```bash
curl http://localhost:8080/actuator/health
```
Expected: `{"status":"UP"}`

### 2. Create an order
```bash
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "cust-001",
    "productId": "prod-abc",
    "quantity": 2
  }'
```
Expected: `201 Created` with an order ID and status `PENDING`.

### 3. Check inventory was updated
```bash
curl http://localhost:8080/api/inventory/prod-abc
```

### 4. Watch the Kafka event chain in Kafka UI
Open http://localhost:8090 and browse topics:
- `order.created` — published by order-service after step 2
- `inventory.updated` — published by inventory-service after reserving stock
- `notif.sent` — published by notification-service after sending confirmation

### 5. Cancel an order
```bash
curl -X DELETE http://localhost:8080/api/orders/{orderId}
```
Watch `order.cancelled` and `order.cancel.requested` topics light up.

---

## Environment Variables (optional overrides)

All services have sensible defaults for local development. Override only when needed.

| Variable | Default | Used by |
|----------|---------|---------|
| `POSTGRES_USER` | `ops` | postgres, all services |
| `POSTGRES_PASSWORD` | `changeme` | postgres, all services |
| `KAFKA_BOOTSTRAP` | `kafka:9092` (Docker) / `localhost:9092` (local) | all services |
| `REDIS_HOST` | `redis` (Docker) / `localhost` (local) | api-gateway, inventory |
| `REDIS_PORT` | `6379` | api-gateway, inventory |
| `JWT_PUBLIC_KEY` | *(required — see JWT Key Setup above)* | api-gateway |
| `HTTP_PORT` | `8081` / `8082` | order-service, inventory-service |
| `SERVER_PORT` | `8083` / `8080` | notification-service, api-gateway |

> **`JWT_PUBLIC_KEY` is required** — generate your key pair and set this variable
> following the "JWT Key Setup" section above before starting any option.

---

## Build Commands Reference

| Goal | Linux/Mac | Windows |
|------|-----------|---------|
| Compile all | `make build` | `.\dev.ps1 build` |
| Run all tests | `make test` | `.\dev.ps1 test` |
| Build fat JARs | `make package` | `.\dev.ps1 package` |
| Build Docker images | `make docker-build` | `.\dev.ps1 docker-build` |
| Full stack up | `make up` | `.\dev.ps1 up` |
| Infra only | `make up-infra` | `.\dev.ps1 up-infra` |
| Stop (keep data) | `make down` | `.\dev.ps1 down` |
| Stop + wipe data | `make down-v` | — |
| View status | `make status` | `.\dev.ps1 status` |
| Clean artifacts | `make clean` | `.\dev.ps1 clean` |

---

## Troubleshooting

**Kafka topics not created / services can't connect to Kafka**
```bash
docker compose logs kafka-init
```
The `kafka-init` container creates topics and exits with code 0. If it shows errors, restart it:
```bash
docker compose up kafka-init
```

**Port already in use**
```bash
# Find what's using port 8080 (Windows)
netstat -ano | findstr :8080

# Find what's using port 8080 (Linux/Mac)
lsof -i :8080
```

**Database migration fails (Flyway)**
Services use Flyway auto-migration on startup. If a migration fails, wipe volumes and restart:
```bash
make down-v
make up
```

> **Important**: Always use `down -v` (wipe volumes) when the Postgres init script changes.
> The `infra/postgres/init-multiple-dbs.sh` script only runs on a fresh volume. Without `-v`,
> existing volumes are reused and newly added databases (`ops_inventory`, `ops_notifications`)
> will not be created, causing Flyway to fail on startup.

**`ops_inventory` or `ops_notifications` database does not exist**
This happens when the Postgres volume was created before the init script ran correctly.
Wipe the volume so the init script re-runs:
```bash
docker compose down -v
docker compose up -d
```
The init script now checks for existing databases before creating them, so `ops_orders`
(created by the Postgres image via `POSTGRES_DB`) is safely skipped.

**`notification-service` fails to start — YAML duplicate key error**
The `application.yml` had a duplicate `url:` key under `spring.datasource`. This has been
fixed. If you see `Caused by: org.yaml.snakeyaml.constructor.DuplicateKeyException`, ensure
you have the latest `notification-service/src/main/resources/application.yml`.

**`api-gateway` fails with `Illegal base64 character 3a` (JWT public key)**
The `JWT_PUBLIC_KEY` env var was defaulting to `classpath:keys/public.pem` — a Spring
Resource path, not a base64 string. This is now handled: the gateway reads the PEM file
from the classpath when the value starts with `classpath:`. A dev RSA key pair is bundled
under `api-gateway/src/main/resources/keys/`. No manual key generation needed for local dev.

**`order-service` fails with `key not found: jackson-json`**
The `pekko-serialization-jackson` dependency was missing from `build.sbt`. This registers
the `jackson-json` serializer alias used in `application.conf` serialization bindings.
Rebuild with:
```bash
sbt "orderService/assembly"
```
or via Docker:
```bash
docker compose up --build -d order-service
```

**Circuit breaker open — 503 responses from gateway**
The gateway circuit breakers trip when a downstream service fails repeatedly.
Check the service is running, then reset via actuator:
```bash
curl -X POST http://localhost:8080/actuator/circuitbreakers/orderCircuitBreaker/reset
```

**Out of memory building Scala services**
```bash
export SBT_OPTS="-Xmx2g -XX:MaxMetaspaceSize=512m"
sbt assembly
```
