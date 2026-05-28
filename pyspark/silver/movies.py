import os
import sys
import shutil

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, trim, initcap,
    year, month, dayofmonth, hour,
    round as spark_round
)

bronze_path     = r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\bronze\movies"
silver_path     = r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\silver\movies"
checkpoint_path = r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\checkpoints\silver\movies"

# ── Debug: check bronze path has data ───────────────────────────────────────
if not os.path.exists(bronze_path):
    print(f"[DEBUG] ERROR: Bronze path does not exist: {bronze_path}")
    print("[DEBUG] Run the bronze writer first before starting silver.")
    sys.exit(1)

bronze_files = [f for f in os.listdir(bronze_path) if f.endswith(".parquet")]
print(f"[DEBUG] Bronze path exists. Parquet files found: {len(bronze_files)}")
if not bronze_files:
    print("[DEBUG] ERROR: No parquet files in bronze path. Nothing to read.")
    sys.exit(1)

# ── Clear stale checkpoint ───────────────────────────────────────────────────
if os.path.exists(checkpoint_path):
    shutil.rmtree(checkpoint_path)
    print(f"[DEBUG] Cleared stale checkpoint: {checkpoint_path}")
else:
    print("[DEBUG] No stale checkpoint found.")

spark = SparkSession.builder \
    .appName("Silver_Movies") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("[DEBUG] Spark session created.")

bronze_schema = """
    key               STRING,
    topic             STRING,
    partition         INT,
    offset            LONG,
    kafka_timestamp   TIMESTAMP,
    booking_id        STRING,
    user              STRING,
    movie             STRING,
    city              STRING,
    seats             INT,
    price             INT,
    booking_timestamp TIMESTAMP,
    ingested_at       TIMESTAMP
"""

print(f"[DEBUG] Reading stream from bronze path: {bronze_path}")
bronze_df = spark.readStream \
    .format("parquet") \
    .schema(bronze_schema) \
    .option("path", bronze_path) \
    .load()

print("[DEBUG] Bronze readStream created. Schema:")
bronze_df.printSchema()

silver_df = bronze_df \
    .filter(col("booking_id").isNotNull()) \
    .filter(col("user").isNotNull()) \
    .filter(col("movie").isNotNull()) \
    .filter(col("seats") > 0) \
    .filter(col("price") > 0) \
    .withColumn("user",  initcap(trim(col("user")))) \
    .withColumn("movie", initcap(trim(col("movie")))) \
    .withColumn("city",  initcap(trim(col("city")))) \
    .withColumn("total_amount", spark_round(col("seats") * col("price"), 2)) \
    .withColumn("booking_year",  year(col("booking_timestamp"))) \
    .withColumn("booking_month", month(col("booking_timestamp"))) \
    .withColumn("booking_day",   dayofmonth(col("booking_timestamp"))) \
    .withColumn("booking_hour",  hour(col("booking_timestamp"))) \
    .select(
        col("booking_id"),
        col("user"),
        col("movie"),
        col("city"),
        col("seats"),
        col("price"),
        col("total_amount"),
        col("booking_timestamp"),
        col("booking_year"),
        col("booking_month"),
        col("booking_day"),
        col("booking_hour"),
        col("kafka_timestamp"),
        col("ingested_at").alias("bronze_ingested_at"),
        current_timestamp().alias("silver_ingested_at")
    )

print("[DEBUG] Silver transformation applied. Schema:")
silver_df.printSchema()

print(f"[DEBUG] Writing to silver path : {silver_path}")
print(f"[DEBUG] Checkpoint path        : {checkpoint_path}")

query = silver_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", silver_path) \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(processingTime="15 seconds") \
    .start()

print("[DEBUG] Silver stream started. Waiting for micro-batches every 15 seconds...")
print("[DEBUG] Press Ctrl+C to stop.\n")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n[DEBUG] KeyboardInterrupt received.")
    print("[DEBUG] Stopping silver stream gracefully...")
    query.stop()
    spark.stop()
    print("[DEBUG] Silver stream stopped.")
    print(f"[DEBUG] Check output at: {silver_path}")