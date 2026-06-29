# ── Scala fat JAR (built by sbt assembly) ─────────────────────────────────────
# Stage 1: Build
FROM eclipse-temurin:21-jdk-alpine AS builder
WORKDIR /build

# Copy only build files first to leverage Docker layer cache
COPY build.sbt project/ ./
# Download dependencies before copying source (cached unless build.sbt changes)
RUN apk add --no-cache bash curl && \
    curl -fsSL https://github.com/sbt/sbt/releases/download/v1.10.1/sbt-1.10.1.tgz \
    | tar xz -C /usr/local && \
    ln -s /usr/local/sbt/bin/sbt /usr/local/bin/sbt

COPY shared/    shared/
COPY order-service/ order-service/

RUN sbt "orderService/assembly" -J-Xmx1g

# Stage 2: Runtime — minimal JRE only
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Non-root user for security (OWASP: avoid running as root)
RUN addgroup -S ops && adduser -S ops -G ops
USER ops

COPY --from=builder /build/order-service/target/scala-3.4.2/order-service.jar app.jar

# Health check — matches K8s readinessProbe
HEALTHCHECK --interval=15s --timeout=5s --start-period=40s --retries=3 \
  CMD wget -qO- http://localhost:8081/health || exit 1

EXPOSE 8081
ENTRYPOINT ["java", \
  "-XX:+UseContainerSupport", \
  "-XX:MaxRAMPercentage=75.0", \
  "-Dfile.encoding=UTF-8", \
  "-jar", "app.jar"]
