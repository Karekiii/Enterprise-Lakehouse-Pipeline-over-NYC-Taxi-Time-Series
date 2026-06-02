#!/usr/bin/env bash
# ============================================================
# CSC5356 Final Project – Bootstrap Script
# Run ONCE after `docker-compose up -d`
# ============================================================
set -euo pipefail

MINIO_ENDPOINT="http://localhost:9000"
MINIO_USER="admin"
MINIO_PASS="password123"
KAFKA_CONNECT="http://localhost:8083"
TRINO="http://localhost:8080"

echo "=== [1/6] Waiting for MinIO to be healthy ==="
until curl -sf "$MINIO_ENDPOINT/minio/health/live"; do
    echo "  MinIO not ready, retrying in 5s…"; sleep 5
done
echo "  ✅ MinIO ready"

echo "=== [2/6] Configuring mc alias ==="
docker run --rm --network docker_lakehouse \
    minio/mc alias set myminio http://minio1:9000 $MINIO_USER $MINIO_PASS

echo "=== [3/6] Creating buckets ==="
docker run --rm --network docker_lakehouse minio/mc \
    mb --ignore-existing myminio/lakehouse
docker run --rm --network docker_lakehouse minio/mc \
    mb --ignore-existing myminio/warehouse
docker run --rm --network docker_lakehouse minio/mc \
    mb --ignore-existing myminio/checkpoints
echo "  ✅ Buckets: lakehouse, warehouse, checkpoints"

echo "=== [4/6] Downloading NYC Taxi dataset ==="
# Download 2024 yellow taxi Parquet files from TLC
mkdir -p ./data/raw
for month in 01 02 03 04 05 06; do
    FILE="yellow_tripdata_2024-${month}.parquet"
    URL="https://d37ci6vzurychx.cloudfront.net/trip-data/$FILE"
    if [ ! -f "./data/raw/$FILE" ]; then
        echo "  Downloading $FILE …"
        curl -L -o "./data/raw/$FILE" "$URL"
    else
        echo "  Already exists: $FILE"
    fi
done
echo "  ✅ Dataset downloaded"

echo "=== [5/6] Uploading dataset to MinIO ==="
for f in ./data/raw/*.parquet; do
    docker run --rm --network docker_lakehouse \
        -v "$(pwd)/data/raw:/data" \
        minio/mc cp "/data/$(basename $f)" myminio/lakehouse/raw/taxi/
done
echo "  ✅ Dataset uploaded to s3://lakehouse/raw/taxi/"

echo "=== [6/6] Registering Kafka Connect S3 Sink ==="
until curl -sf "$KAFKA_CONNECT/connectors" > /dev/null; do
    echo "  Kafka Connect not ready, retrying…"; sleep 5
done
curl -X POST "$KAFKA_CONNECT/connectors" \
     -H "Content-Type: application/json" \
     -d @config/kafka-connect-s3-sink.json
echo ""
echo "  ✅ Kafka Connect S3 Sink registered"

echo ""
echo "=========================================="
echo "  Bootstrap complete!"
echo "  MinIO Console : http://localhost:9001"
echo "  Trino UI      : http://localhost:8080"
echo "  Spark Master  : http://localhost:8082"
echo "  Grafana       : http://localhost:3000"
echo "  Iceberg REST  : http://localhost:8181"
echo "=========================================="
echo ""
echo "Next steps:"
echo "  1. Run Spark job to create Iceberg tables:"
echo "     docker exec spark-master spark-submit /opt/bitnami/spark/jobs/01_create_tables.py"
echo "  2. Run ingestion job:"
echo "     docker exec spark-master spark-submit /opt/bitnami/spark/jobs/02_streaming_ingestion.py"
echo "  3. Run aggregation + anomaly job:"
echo "     docker exec spark-master spark-submit /opt/bitnami/spark/jobs/03_aggregation_anomaly.py"
echo "  4. Open Trino and run queries from queries/analytics_queries.sql"
