"""
CSC5356 Final Project
Step 2: Ingest raw taxi trip data from MinIO -> Iceberg
        Fixed for 2024 schema, optimized for memory
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, year, month, dayofmonth, hour,
    monotonically_increasing_id, lit
)

spark = (
    SparkSession.builder
    .appName("CSC5356-TaxiIngestion")
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
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio1:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.region", "us-east-1")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .config("spark.sql.shuffle.partitions", "20")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

RAW_PATH = "s3a://lakehouse/raw/taxi/"

print("=" * 60)
print("Reading Parquet files from MinIO ...")
print("=" * 60)

raw_df = spark.read.parquet(RAW_PATH)
print(f"Raw row count: {raw_df.count()}")

print("Cleaning and enriching data ...")

cleaned = (
    raw_df
    .filter(col("fare_amount") > 0)
    .filter(col("trip_distance") > 0)
    .filter(col("passenger_count").between(1, 8))
    .withColumn("pickup_datetime",  to_timestamp("tpep_pickup_datetime"))
    .withColumn("dropoff_datetime", to_timestamp("tpep_dropoff_datetime"))
    .filter(col("pickup_datetime").isNotNull())
    .withColumn("trip_id",      monotonically_increasing_id())
    .withColumn("pickup_year",  year("pickup_datetime"))
    .withColumn("pickup_month", month("pickup_datetime"))
    .withColumn("pickup_day",   dayofmonth("pickup_datetime"))
    .withColumn("pickup_hour",  hour("pickup_datetime"))
    .select(
        col("trip_id"),
        col("VendorID").alias("vendor_id"),
        col("pickup_datetime"),
        col("dropoff_datetime"),
        col("passenger_count"),
        col("trip_distance"),
        lit(None).cast("double").alias("pickup_longitude"),
        lit(None).cast("double").alias("pickup_latitude"),
        lit(None).cast("double").alias("dropoff_longitude"),
        lit(None).cast("double").alias("dropoff_latitude"),
        col("RatecodeID").alias("rate_code"),
        col("payment_type"),
        col("fare_amount"),
        col("extra"),
        col("mta_tax"),
        col("tip_amount"),
        col("tolls_amount"),
        col("total_amount"),
        col("pickup_year"),
        col("pickup_month"),
        col("pickup_day"),
        col("pickup_hour"),
    )
)

print("Writing to Iceberg table rest.timeseries.taxi_trips ...")
cleaned.writeTo("rest.timeseries.taxi_trips").append()

print("=" * 60)
print("SUCCESS: Data written to Iceberg.")
print("=" * 60)

print("Verifying row count in Iceberg table ...")
spark.sql("SELECT COUNT(*) AS total FROM rest.timeseries.taxi_trips").show()

spark.stop()
