FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /build
COPY services/security /build/services/security
RUN mvn -B -q -f services/security/pom.xml install -DskipTests
COPY services/ingestion /build/services/ingestion
RUN mvn -B -q -f services/ingestion/pom.xml package -DskipTests
FROM eclipse-temurin:21-jre-jammy
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /build/services/ingestion/target/ingestion-1.0.0.jar app.jar
COPY config config
ENV CONFIG_PATH=/app/config/ingestion.json MEDIA_ROOT=/data/media
EXPOSE 8080
ENTRYPOINT ["java","-jar","app.jar"]
