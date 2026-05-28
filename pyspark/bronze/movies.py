import os
import sys
import shutil

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, TimestampType
)

landing_path    = r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\landing\movies"
bronze_path     = r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\bronze\movies"
checkpoint_path = r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\checkpoints\bronze\movies"

# ── Debug: check landing path has data ──────────────────────────────────────
if not os.path.exists(landing_path):
    print(f"[DEBUG] ERROR: Landing path does not exist: {landing_path}")
    print("[DEBUG] Run the landing writer first before starting bronze.")
    sys.exit(1)

landing_files = [f for f in os.listdir(landing_path) if f.endswith(".parquet")]
print(f"[DEBUG] Landing path exists. Parquet files found: {len(landing_files)}")
if not landing_files:
    print("[DEBUG] ERROR: No parquet files in landing path. Nothing to read.")
    sys.exit(1)

# ── Clear stale checkpoint ───────────────────────────────────────────────────
if os.path.exists(checkpoint_path):
    shutil.rmtree(checkpoint_path)
    print(f"[DEBUG] Cleared stale checkpoint: {checkpoint_path}")
else:
    print(f"[DEBUG] No stale checkpoint found.")

spark = SparkSession.builder \
    .appName("Bronze_Movies") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("[DEBUG] Spark session created.")

movie_schema = StructType([
    StructField("booking_id", StringType(),  True),
    StructField("user",       StringType(),  True),
    StructField("movie",      StringType(),  True),
    StructField("city",       StringType(),  True),
    StructField("seats",      IntegerType(), True),
    StructField("price",      IntegerType(), True),
    StructField("timestamp",  StringType(),  True)
])

landing_schema = "key STRING, value STRING, topic STRING, " \
                 "partition INT, offset LONG, timestamp TIMESTAMP"

print(f"[DEBUG] Reading stream from landing path: {landing_path}")
landing_df = spark.readStream \
    .format("parquet") \
    .schema(landing_schema) \
    .option("path", landing_path) \
    .load()

print("[DEBUG] Landing readStream created. Schema:")
landing_df.printSchema()

bronze_df = landing_df \
    .withColumn("payload", from_json(col("value"), movie_schema)) \
    .select(
        col("key"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),

        col("payload.booking_id"),
        col("payload.user"),
        col("payload.movie"),
        col("payload.city"),
        col("payload.seats"),
        col("payload.price"),
        col("payload.timestamp").cast("timestamp").alias("booking_timestamp"),

        current_timestamp().alias("ingested_at")
    )

print("[DEBUG] Bronze transformation applied. Schema:")
bronze_df.printSchema()

print(f"[DEBUG] Writing to bronze path     : {bronze_path}")
print(f"[DEBUG] Checkpoint path            : {checkpoint_path}")

query = bronze_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", bronze_path) \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(processingTime="10 seconds") \
    .start()

print("[DEBUG] Bronze stream started. Waiting for micro-batches every 10 seconds...")
print("[DEBUG] Press Ctrl+C to stop.\n")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n[DEBUG] KeyboardInterrupt received.")
    print("[DEBUG] Stopping bronze stream gracefully...")
    query.stop()
    spark.stop()
    print("[DEBUG] Bronze stream stopped.")
    print(f"[DEBUG] Check output at: {bronze_path}")