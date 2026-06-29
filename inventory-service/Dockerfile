# ── Scala fat JAR (built by sbt assembly) ─────────────────────────────────────
# Stage 1: Build
FROM eclipse-temurin:21-jdk-alpine AS builder
WORKDIR /build

COPY build.sbt project/ ./
RUN apk add --no-cache bash curl && \
    curl -fsSL https://github.com/sbt/sbt/releases/download/v1.10.1/sbt-1.10.1.tgz \
    | tar xz -C /usr/local && \
    ln -s /usr/local/sbt/bin/sbt /usr/local/bin/sbt

COPY shared/          shared/
COPY inventory-service/ inventory-service/

RUN sbt "inventoryService/assembly" -J-Xmx1g

# Stage 2: Runtime
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

RUN addgroup -S ops && adduser -S ops -G ops
USER ops

COPY --from=builder /build/inventory-service/target/scala-3.4.2/inventory-service.jar app.jar

HEALTHCHECK --interval=15s --timeout=5s --start-period=40s --retries=3 \
  CMD wget -qO- http://localhost:8082/health || exit 1

EXPOSE 8082
ENTRYPOINT ["java", \
  "-XX:+UseContainerSupport", \
  "-XX:MaxRAMPercentage=75.0", \
  "-Dfile.encoding=UTF-8", \
  "-jar", "app.jar"]
