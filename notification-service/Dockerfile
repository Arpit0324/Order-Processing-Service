# ── Maven Spring Boot fat JAR ─────────────────────────────────────────────────
# Stage 1: Build
FROM eclipse-temurin:21-jdk-alpine AS builder
WORKDIR /build

# Cache Maven dependencies separately from source
COPY pom.xml ./
COPY notification-service/pom.xml notification-service/
RUN apk add --no-cache maven && \
    mvn -pl notification-service dependency:go-offline -q

COPY notification-service/src notification-service/src
RUN mvn -pl notification-service package -DskipTests -q

# Stage 2: Runtime — layered Spring Boot JAR for better layer caching
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

RUN addgroup -S ops && adduser -S ops -G ops

# Extract layered Spring Boot JAR for optimized image layers
COPY --from=builder /build/notification-service/target/notification-service-*.jar app.jar
RUN java -Djarmode=layertools -jar app.jar extract --destination extracted

USER ops

HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 \
  CMD wget -qO- http://localhost:8083/actuator/health || exit 1

EXPOSE 8083
ENTRYPOINT ["java", \
  "-XX:+UseContainerSupport", \
  "-XX:MaxRAMPercentage=75.0", \
  "-Dfile.encoding=UTF-8", \
  "-jar", "app.jar"]
