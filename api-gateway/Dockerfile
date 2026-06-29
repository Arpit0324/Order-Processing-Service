# ── Maven Spring Boot fat JAR (Spring Cloud Gateway / WebFlux) ───────────────
# Stage 1: Build
FROM eclipse-temurin:21-jdk-alpine AS builder
WORKDIR /build

COPY pom.xml ./
COPY api-gateway/pom.xml api-gateway/
RUN apk add --no-cache maven && \
    mvn -pl api-gateway dependency:go-offline -q

COPY api-gateway/src api-gateway/src
RUN mvn -pl api-gateway package -DskipTests -q

# Stage 2: Runtime
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

RUN addgroup -S ops && adduser -S ops -G ops
USER ops

COPY --from=builder /build/api-gateway/target/api-gateway-*.jar app.jar

HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 \
  CMD wget -qO- http://localhost:8080/actuator/health || exit 1

EXPOSE 8080
ENTRYPOINT ["java", \
  "-XX:+UseContainerSupport", \
  "-XX:MaxRAMPercentage=75.0", \
  "-Dfile.encoding=UTF-8", \
  "-jar", "app.jar"]
