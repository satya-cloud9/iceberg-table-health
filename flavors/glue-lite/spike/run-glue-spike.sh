#!/usr/bin/env bash
# GL0 Glue spike: run Spark in a throwaway Docker container against the
# Floci emulator (localhost:4566) and check whether Iceberg's GlueCatalog
# can create a table and commit to it. Nothing is installed into the
# Kubernetes cluster -- this isolates the Glue question from cluster plumbing.
#
# Usage (from repo root, with lakehouse-floci running):
#   bash flavors/glue-lite/spike/run-glue-spike.sh
#
# Go/no-go:
#   PASS -> the last three queries print 5 rows, 2 'append' snapshots and
#           3 partitions, and the Glue table shows a metadata_location.
#   FAIL -> any error on CREATE TABLE or the second INSERT (that's the
#           conditional glue:UpdateTable every Iceberg commit relies on).
set -euo pipefail

cd "$(dirname "$0")"

ENDPOINT="${FLOCI_ENDPOINT:-http://localhost:4566}"
BUCKET="${WAREHOUSE_BUCKET:-glue-lite-warehouse}"
REGION="${AWS_REGION:-us-east-1}"
SPARK_IMAGE="${SPARK_IMAGE:-apache/spark:3.5.6}"
ICEBERG_VERSION="${ICEBERG_VERSION:-1.10.0}"

# Floci accepts any static credentials (same convention as LocalStack).
AWS_ENV=(-e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test
         -e AWS_REGION="$REGION" -e AWS_DEFAULT_REGION="$REGION")

aws_floci() {
  docker run --rm --network host "${AWS_ENV[@]}" amazon/aws-cli \
    --endpoint-url "$ENDPOINT" "$@"
}

echo "=== Checking Floci at $ENDPOINT ==="
curl -fs "$ENDPOINT/_localstack/health" >/dev/null \
  || { echo "Floci not reachable at $ENDPOINT -- run 'make emulator-up' first." >&2; exit 1; }

echo "=== Creating warehouse bucket s3://$BUCKET (ok if it already exists) ==="
aws_floci s3 mb "s3://$BUCKET" 2>/dev/null || true

echo "=== Running glue_spike.sql in $SPARK_IMAGE with Iceberg $ICEBERG_VERSION ==="
docker run --rm --network host "${AWS_ENV[@]}" \
  -v "$PWD/glue_spike.sql:/tmp/glue_spike.sql:ro" \
  "$SPARK_IMAGE" \
  /opt/spark/bin/spark-sql \
    --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:${ICEBERG_VERSION},org.apache.iceberg:iceberg-aws-bundle:${ICEBERG_VERSION}" \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --conf spark.ui.enabled=false \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.glue=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.glue.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
    --conf spark.sql.catalog.glue.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.glue.warehouse="s3://$BUCKET/warehouse" \
    --conf spark.sql.catalog.glue.glue.endpoint="$ENDPOINT" \
    --conf spark.sql.catalog.glue.s3.endpoint="$ENDPOINT" \
    --conf spark.sql.catalog.glue.s3.path-style-access=true \
    --conf spark.sql.catalog.glue.client.region="$REGION" \
    -f /tmp/glue_spike.sql

echo ""
echo "=== Glue's view of the table (metadata_location should point into s3://$BUCKET) ==="
aws_floci glue get-table --database-name demo --name spike_events \
  --query 'Table.Parameters' --output json

echo ""
echo "=== Files written to S3 ==="
aws_floci s3 ls "s3://$BUCKET/warehouse/" --recursive | tail -20
