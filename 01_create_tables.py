"""
CSC5356 Final Project
Step 1: Create Iceberg Tables with Parquet format + partitioning
Dataset: NYC Yellow Taxi Trip Records (>500MB, time series)
         https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, hour, to_timestamp

# ── Spark Session ────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("CSC5356-IcebergInit")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.rest",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.rest.type", "rest")
    .config("spark.sql.catalog.rest.uri", "http://iceberg-rest:8181")
    .config("spark.sql.catalog.rest.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.rest.s3.endpoint", "http://minio1:9000")
    .config("spark.sql.catalog.rest.s3.access-key-id", "admin")
    .config("spark.sql.catalog.rest.s3.secret-access-key", "password123")
    .config("spark.sql.catalog.rest.s3.path-style-access", "true")
    .config("spark.sql.catalog.rest.s3.region", "us-east-1")
    .config("spark.sql.catalog.rest.warehouse", "s3://warehouse/")
    # S3A / MinIO credentials
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio1:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.region", "us-east-1")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    # Parquet defaults
    .config("spark.sql.parquet.compression.codec", "snappy")
    .config("spark.sql.parquet.writeLegacyFormat", "false")
    # Iceberg write tuning
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS rest.timeseries")

# ── TABLE 1: Raw taxi trips ──────────────────────────────────────────────────
spark.sql("""
CREATE TABLE IF NOT EXISTS rest.timeseries.taxi_trips (
    trip_id             BIGINT        COMMENT 'Synthetic surrogate key',
    vendor_id           INT           COMMENT 'TPEP provider: 1=Creative Mobile, 2=VeriFone',
    pickup_datetime     TIMESTAMP     COMMENT 'Meter engaged timestamp',
    dropoff_datetime    TIMESTAMP     COMMENT 'Meter disengaged timestamp',
    passenger_count     INT           COMMENT 'Number of passengers (driver-entered)',
    trip_distance       DOUBLE        COMMENT 'Miles reported by taximeter',
    pickup_longitude    DOUBLE,
    pickup_latitude     DOUBLE,
    dropoff_longitude   DOUBLE,
    dropoff_latitude    DOUBLE,
    rate_code           INT           COMMENT '1=Standard, 2=JFK, 3=Newark, 4=Nassau, 5=Negotiated, 6=Group',
    payment_type        INT           COMMENT '1=Credit, 2=Cash, 3=No charge, 4=Dispute',
    fare_amount         DOUBLE,
    extra               DOUBLE        COMMENT 'Rush hour / overnight surcharges',
    mta_tax             DOUBLE,
    tip_amount          DOUBLE,
    tolls_amount        DOUBLE,
    total_amount        DOUBLE,
    pickup_year         INT,
    pickup_month        INT,
    pickup_day          INT,
    pickup_hour         INT
)
USING iceberg
PARTITIONED BY (pickup_year, pickup_month, pickup_day)
TBLPROPERTIES (
    'write.format.default'                  = 'parquet',
    'write.parquet.compression-codec'       = 'snappy',
    'write.target-file-size-bytes'          = '134217728',
    'write.distribution-mode'               = 'hash',

    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max'       = '10',

    'history.expire.max-snapshot-age-ms'    = '604800000',

    'read.parquet.vectorization.enabled'    = 'true',
    'read.split.target-size'                = '134217728'
)
""")

# ── TABLE 2: Hourly aggregates (materialized via Spark, queried via Trino) ──
spark.sql("""
CREATE TABLE IF NOT EXISTS rest.timeseries.taxi_hourly_agg (
    pickup_year         INT,
    pickup_month        INT,
    pickup_day          INT,
    pickup_hour         INT,
    total_trips         BIGINT,
    total_passengers    BIGINT,
    avg_trip_distance   DOUBLE,
    avg_fare_amount     DOUBLE,
    avg_tip_amount      DOUBLE,
    total_revenue       DOUBLE,
    p95_fare            DOUBLE        COMMENT '95th percentile fare (outlier detection)',
    computed_at         TIMESTAMP
)
USING iceberg
PARTITIONED BY (pickup_year, pickup_month)
TBLPROPERTIES (
    'write.format.default'            = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'write.target-file-size-bytes'    = '67108864'
)
""")

# ── TABLE 3: Anomaly events detected by streaming ──────────────────────────
spark.sql("""
CREATE TABLE IF NOT EXISTS rest.timeseries.anomaly_events (
    event_id            STRING        COMMENT 'UUID',
    detected_at         TIMESTAMP,
    anomaly_type        STRING        COMMENT 'surge_fare | low_tip | long_idle | outlier_distance',
    severity            STRING        COMMENT 'LOW | MEDIUM | HIGH',
    pickup_datetime     TIMESTAMP,
    fare_amount         DOUBLE,
    reference_avg       DOUBLE        COMMENT 'Expected value at that hour',
    z_score             DOUBLE        COMMENT 'Standard deviations from mean',
    event_year          INT,
    event_month         INT
)
USING iceberg
PARTITIONED BY (event_year, event_month)
TBLPROPERTIES (
    'write.format.default'            = 'parquet',
    'write.parquet.compression-codec' = 'snappy'
)
""")

print("✅ All Iceberg tables created successfully")
spark.sql("SHOW TABLES IN rest.timeseries").show()
