FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY services/security /build/security
RUN mvn -B -ntp -f /build/security/pom.xml install -DskipTests
COPY services/query/pom.xml /build/query/pom.xml
COPY services/query/src /build/query/src
RUN mvn -B -ntp -f /build/query/pom.xml package -DskipTests
RUN DUCKDB_EXTENSION_DIRECTORY=/opt/duckdb-extensions java -cp /build/query/target/query-0.1.0-SNAPSHOT.jar io.contextgraph.query.ExtensionInstaller
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/query/target/query-0.1.0-SNAPSHOT.jar /app/query.jar
COPY --from=build /opt/duckdb-extensions /opt/duckdb-extensions
COPY config /app/config
ENV QUERY_CONFIG=/app/config/query.yaml DUCKDB_EXTENSION_DIRECTORY=/opt/duckdb-extensions
EXPOSE 8081
ENTRYPOINT ["java","-jar","/app/query.jar"]
