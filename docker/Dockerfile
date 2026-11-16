# ---------- Stage 1: resolve deps + build (Maven + JDK 11) ----------
FROM maven:3.9-eclipse-temurin-11 AS build
WORKDIR /build

# Copy ONLY the POM first — this layer caches all Maven Central downloads
COPY pom.xml .
RUN mvn -B -e dependency:go-offline

# Sources come after, so editing code doesn't re-download dependencies
RUN mvn -B package -DskipTests \
    && mvn -B dependency:copy-dependencies -DoutputDirectory=target/lib

# ---------- Stage 2: runtime (JDK 11) ----------
FROM eclipse-temurin:11-jdk-jammy

WORKDIR /app
COPY --from=build /build/target/lib /app/lib

# Connection string for configuration server. Override at runtime with -e LLCONFIG=...
# LOGGER_CONFIG and CONFIG_FILE_URI are derived from this in entrypoint.sh.
ENV LLCONFIG=http://lablink-config:10101/get?id=

# Sync host configuration id (appended to LLCONFIG).
ENV SYNC_CONFIG=lablink-sync-host.properties

# Path to Java JAR file of sync host.
ENV SYNC_JAR_FILE=/app/lib/sync-0.0.2-jar-with-dependencies.jar

# Specify the maximum size (in bytes) of the memory allocation pool.
ENV MEMORY_FLAG=-Xmx512m

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Scenario JSON named by syncHost.syncScenarioFile.<scenario> is read from the
# working directory. Mount that file into /app at runtime.
ENTRYPOINT ["/entrypoint.sh"]
