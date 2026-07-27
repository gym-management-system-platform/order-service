# syntax=docker/dockerfile:1
# Build context must be the monorepo root (see docker-compose.yaml).
FROM eclipse-temurin:21-jdk AS builder

WORKDIR /app

COPY gradlew .
COPY gradle gradle
COPY build.gradle ./
COPY kafka-starter kafka-starter
COPY global-exception-starter global-exception-starter
COPY order-service order-service

# Root settings.gradle also includes inventory/payment — keep only modules we copy.
RUN printf '%s\n' \
    "rootProject.name = 'fitness-system-saga-orchestration'" \
    "include 'kafka-starter'" \
    "include 'global-exception-starter'" \
    "include 'order-service'" \
    > settings.gradle

# mydev.logging lives in local ~/.m2 (mavenLocal), not on Maven Central.
# Compose passes host ~/.m2 as additional context "m2".
RUN --mount=type=bind,from=m2,source=.,target=/root/.m2,ro \
    chmod +x ./gradlew \
    && ./gradlew :order-service:clean :order-service:bootJar -x test --no-daemon \
    && cp order-service/build/libs/$(ls order-service/build/libs | grep -v plain | head -1) /app/app.jar

FROM eclipse-temurin:21-jre-jammy

WORKDIR /app

COPY --from=builder /app/app.jar app.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]
