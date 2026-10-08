# Spark image for every Glue-Lite job: stock Apache Spark plus the Iceberg
# runtime and Iceberg AWS bundle (GlueCatalog + S3FileIO), with the job
# scripts baked in. Same versions the GL0 spike passed with on the host.
#
# Build (from repo root):
#   docker build -f flavors/glue-lite/docker/spark.Dockerfile -t glue-lite-spark:local .
ARG SPARK_IMAGE=apache/spark:3.5.6
FROM ${SPARK_IMAGE}

ARG ICEBERG_VERSION=1.10.0
ARG MAVEN=https://repo1.maven.org/maven2/org/apache/iceberg

USER root
RUN set -eux; \
    cd /opt/spark/jars; \
    curl -fsSLO "${MAVEN}/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"; \
    curl -fsSLO "${MAVEN}/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"

# boto3 for the run coordinator's claims in DynamoDB (coordinator.py) and the
# RDS Data API; pg8000 (pure Python) for the Postgres backends (pg_store.py)
RUN set -eux; \
    (python3 -m pip --version >/dev/null 2>&1 || (apt-get update && apt-get install -y --no-install-recommends python3-pip && rm -rf /var/lib/apt/lists/*)); \
    python3 -m pip install --no-cache-dir "boto3>=1.34" "pg8000>=1.31"

COPY flavors/glue-lite/jobs/ /opt/jobs/

# Back to the image's non-root spark user (uid 185).
USER 185
