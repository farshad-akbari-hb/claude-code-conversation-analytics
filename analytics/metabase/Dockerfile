# Custom Metabase image with DuckDB support
# Based on MotherDuck's recommended approach for glibc compatibility
# See: https://github.com/MotherDuck-Open-Source/metabase_duckdb_driver

FROM eclipse-temurin:21-jre-jammy

# Build arguments for versions
# Using MotherDuck-recommended version for DuckDB driver compatibility
ARG METABASE_VERSION=0.56.9
ARG METABASE_DUCKDB_DRIVER_VERSION=1.4.3.0

ENV MB_PLUGINS_DIR=/home/metabase/plugins/

# Create metabase user
RUN groupadd -r metabase && useradd -r -g metabase metabase

# Install CA certificates for HTTPS downloads and curl for health checks
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create directories for plugins and data
RUN mkdir -p /home/metabase/plugins /home/metabase/data /duckdb && \
    chown -R metabase:metabase /home/metabase /duckdb

WORKDIR /home/metabase

# Download Metabase JAR
ADD --chown=metabase:metabase https://downloads.metabase.com/v${METABASE_VERSION}/metabase.jar /home/metabase/

# Download DuckDB driver
ADD --chown=metabase:metabase https://github.com/MotherDuck-Open-Source/metabase_duckdb_driver/releases/download/${METABASE_DUCKDB_DRIVER_VERSION}/duckdb.metabase-driver.jar /home/metabase/plugins/

# Ensure proper file permissions
RUN chmod 755 /home/metabase/metabase.jar && \
    chmod 755 /home/metabase/plugins/duckdb.metabase-driver.jar

EXPOSE 3000

USER metabase

CMD ["java", "-jar", "/home/metabase/metabase.jar"]
