"""
CSC5356 Final Project
Step 3: Batch aggregation + anomaly detection
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as _sum, avg, percentile_approx,
    current_timestamp, expr, stddev, abs as _abs
)

spark = (
    SparkSession.builder
    .appName("CSC5356-Aggregation")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.rest",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.rest.type",             "rest")
    .config("spark.sql.catalog.rest.uri",              "http://iceberg-rest:8181")
    .config("spark.sql.catalog.rest.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.rest.s3.endpoint",     "http://minio1:9000")
    .config("spark.sql.catalog.rest.s3.access-key-id","admin")
    .config("spark.sql.catalog.rest.s3.secret-access-key","password123")
    .config("spark.sql.catalog.rest.s3.path-style-access","true")
    .config("spark.sql.catalog.rest.s3.region",       "us-east-1")
    .config("spark.sql.catalog.rest.warehouse",        "s3://warehouse/")
    .config("spark.hadoop.fs.s3a.endpoint",            "http://minio1:9000")
    .config("spark.hadoop.fs.s3a.access.key",          "admin")
    .config("spark.hadoop.fs.s3a.secret.key",          "password123")
    .config("spark.hadoop.fs.s3a.path.style.access",   "true")
    .config("spark.hadoop.fs.s3a.region",              "us-east-1")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.shuffle.partitions", "20")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ── 1. Hourly Aggregations ─────────────────────────────────────────────────
print("Computing hourly aggregations ...")

hourly = spark.sql("""
    SELECT
        pickup_year,
        pickup_month,
        pickup_day,
        pickup_hour,
        COUNT(*)                                        AS total_trips,
        SUM(passenger_count)                            AS total_passengers,
        ROUND(AVG(trip_distance),    3)                 AS avg_trip_distance,
        ROUND(AVG(fare_amount),      2)                 AS avg_fare_amount,
        ROUND(AVG(tip_amount),       2)                 AS avg_tip_amount,
        ROUND(SUM(total_amount),     2)                 AS total_revenue,
        ROUND(percentile_approx(fare_amount, 0.95), 2)  AS p95_fare,
        current_timestamp()                             AS computed_at
    FROM rest.timeseries.taxi_trips
    GROUP BY pickup_year, pickup_month, pickup_day, pickup_hour
""")

hourly.writeTo("rest.timeseries.taxi_hourly_agg").createOrReplace()
print(f"Hourly aggregations written: {hourly.count()} rows")

# ── 2. Anomaly Detection using Z-score ─────────────────────────────────────
print("Running anomaly detection ...")

hour_stats = spark.sql("""
    SELECT
        pickup_hour,
        AVG(fare_amount)    AS mean_fare,
        STDDEV(fare_amount) AS std_fare
    FROM rest.timeseries.taxi_trips
    GROUP BY pickup_hour
""")

trips = spark.table("rest.timeseries.taxi_trips")
trips_with_stats = trips.join(hour_stats, on="pickup_hour", how="left")

anomalies = (
    trips_with_stats
    .withColumn("z_score",
        (col("fare_amount") - col("mean_fare")) / col("std_fare"))
    .filter(_abs(col("z_score")) > 3.0)
    .withColumn("event_id",      expr("uuid()"))
    .withColumn("detected_at",   current_timestamp())
    .withColumn("anomaly_type",
        expr("""
            CASE
              WHEN z_score > 3  THEN 'surge_fare'
              WHEN z_score < -3 THEN 'low_fare'
              ELSE 'outlier_distance'
            END
        """))
    .withColumn("severity",
        expr("""
            CASE
              WHEN ABS(z_score) > 6 THEN 'HIGH'
              WHEN ABS(z_score) > 4 THEN 'MEDIUM'
              ELSE 'LOW'
            END
        """))
    .withColumn("reference_avg", col("mean_fare"))
    .withColumn("event_year",    col("pickup_year"))
    .withColumn("event_month",   col("pickup_month"))
    .select(
        "event_id", "detected_at", "anomaly_type", "severity",
        "pickup_datetime", "fare_amount", "reference_avg", "z_score",
        "event_year", "event_month"
    )
)

anomalies.writeTo("rest.timeseries.anomaly_events").append()
anom_count = anomalies.count()
print(f"Anomalies detected and written: {anom_count} events")

# ── 3. Iceberg maintenance ──────────────────────────────────────────────────
print("Running Iceberg maintenance ...")
spark.sql("""
    CALL rest.system.rewrite_data_files(
        table    => 'rest.timeseries.taxi_trips',
        strategy => 'binpack',
        options  => map('target-file-size-bytes','134217728',
                        'min-input-files','3')
    )
""")
print("Iceberg maintenance complete")

print("=" * 60)
print("All done!")
print("=" * 60)

spark.sql("SELECT COUNT(*) AS hourly_rows  FROM rest.timeseries.taxi_hourly_agg").show()
spark.sql("SELECT COUNT(*) AS anomaly_rows FROM rest.timeseries.anomaly_events").show()

spark.stop()
