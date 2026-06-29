# API Gateway

The single entry point for all client requests. Built with **Java 21 + Spring Cloud Gateway** (reactive, non-blocking). Handles authentication, rate limiting, routing, circuit breaking, and serves the Swagger UI.

---

## Responsibilities

- **JWT Authentication** — validate RS256 tokens, extract user identity
- **Rate Limiting** — Redis-based sliding window (100 req/min per IP)
- **Routing** — proxy requests to order-service, inventory-service, notification-service
- **Circuit Breaker** — Resilience4j per downstream; open after 50% failure rate
- **Retry** — automatic GET retries on `502 / 503`
- **Swagger UI** — aggregates OpenAPI specs from all services

---

## Filter Chain

Every request passes through these global filters in order:

```
Request
  │
  ▼ JwtAuthFilter (order -200)
  │  • Validate RS256 Bearer token
  │  • Reject 401 if missing/invalid/expired
  │  • Inject X-User-Id header for downstream services
  │  • Bypass: /swagger-ui, /v3/api-docs, /actuator/health
  │
  ▼ RequestLoggingFilter (order -100)
  │  • Generate X-Trace-Id (UUID) if not present
  │  • Inject X-Trace-Id into downstream request
  │  • Log: method, path, userId, response time, status
  │
  ▼ RateLimitFilter (order -50)
  │  • Redis INCR ops:ratelimit:{clientIp} TTL 60s
  │  • 429 Too Many Requests if > 100/min
  │  • Fails open if Redis is down
  │
  ▼ Spring Cloud Gateway Route Matching
  │  • CircuitBreaker + Retry filters per route
  │  • Fallback: /fallback/{service} → 503 JSON
  │
  ▼ Downstream Service
```

---

## Technology

| Component | Technology |
|-----------|-----------|
| Language | Java 21 |
| Framework | Spring Boot 3.3.1 + Spring Cloud Gateway 2023.0.2 |
| Circuit breaker | Resilience4j (Reactor) |
| JWT | JJWT 0.12.6 (RS256) |
| Rate limiting | Redis via Spring Data Redis Reactive |
| API docs | SpringDoc OpenAPI 2.5.0 + Swagger UI |

---

## Running Locally

### With Docker Compose (recommended)

```bash
# From project root
docker-compose up api-gateway redis kafka
```

### Standalone

```bash
# From project root — requires Redis running and downstream services available
mvn -pl api-gateway spring-boot:run
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER_PORT` | `8080` | HTTP server port |
| `ORDER_SERVICE_URL` | `http://localhost:8081` | Order Service URL |
| `INVENTORY_SERVICE_URL` | `http://localhost:8082` | Inventory Service URL |
| `NOTIFICATION_SERVICE_URL` | `http://localhost:8083` | Notification Service URL |
| `REDIS_HOST` | `localhost` | Redis hostname |
| `REDIS_PORT` | `6379` | Redis port |
| `JWT_PUBLIC_KEY` | — | RS256 public key (base64 DER, no PEM headers) |

---

## Important URLs

| URL | Description |
|-----|-------------|
| http://localhost:8080/swagger-ui.html | **Swagger UI** — try all endpoints |
| http://localhost:8080/v3/api-docs | OpenAPI 3.0 JSON spec |
| http://localhost:8080/actuator/health | Liveness probe |
| http://localhost:8080/actuator/health/readiness | Readiness probe |
| http://localhost:8080/actuator/circuitbreakers | Circuit breaker state |
| http://localhost:8080/actuator/metrics | Prometheus metrics |

---

## Route Table

| Route | Upstream | Circuit Breaker | Retry (GET) |
|-------|----------|----------------|-------------|
| `/api/orders/**` | `order-service:8081` | `orderCircuitBreaker` | 2× |
| `/api/inventory/**` | `inventory-service:8082` | `inventoryCircuitBreaker` | 2× |
| `/api/notifications/**` | `notification-service:8083` | `notificationCircuitBreaker` | — |

---

## Circuit Breaker Config

```
Sliding window: 10 requests
Failure threshold: 50% → OPEN
Wait in OPEN state: 30s
Half-open: allow 3 test requests
Timeout per call: 5s
```

Notification service uses relaxed settings: 60% failure threshold, 60s wait.

---

## JWT Setup

The gateway validates RS256 JWTs. Generate keys:

```bash
# Generate RSA key pair
openssl genrsa -out private.pem 2048
openssl rsa -in private.pem -pubout -out public.pem

# Convert public key to base64 DER (for JWT_PUBLIC_KEY env var)
openssl rsa -in private.pem -pubout -outform DER | base64 -w0
```

Set the output as `JWT_PUBLIC_KEY` in `.env`. The private key stays with your auth service — the gateway only needs the public key.

---

## Testing Without a Real JWT (Local Dev)

For local testing, you can temporarily disable `JwtAuthFilter` by adding your test path to `BYPASS_PATHS` in `JwtAuthFilter.java`, or add a dev profile that skips the filter:

```bash
# Or just test via Swagger UI — it has an Authorize button for Bearer tokens
open http://localhost:8080/swagger-ui.html
```

---

## Build

```bash
# From project root
mvn -pl api-gateway package -DskipTests
mvn -pl api-gateway test
docker build -t ops/api-gateway ./api-gateway
```
