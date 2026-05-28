import os
import sys

# ================= WINDOWS FIX (must be before SparkSession) =================
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType
)

# ================= SPARK SESSION =================
spark = SparkSession.builder \
    .appName("Bronze_IPL2026") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.local.dir", "C:/tmp/spark-temp") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ================= SCHEMA (matched to your IPL Kafka JSON payload) =================
# match_id, team1, team2, stadium, tickets_sold, price, timestamp
ipl_schema = StructType([
    StructField("match_id",     StringType(),  True),
    StructField("team1",        StringType(),  True),
    StructField("team2",        StringType(),  True),
    StructField("stadium",      StringType(),  True),
    StructField("tickets_sold", IntegerType(), True),
    StructField("price",        IntegerType(), True),
    StructField("timestamp",    StringType(),  True)  # string in producer, cast below
])

# ================= LANDING SCHEMA =================
landing_schema = "key STRING, value STRING, topic STRING, " \
                 "partition INT, offset LONG, timestamp TIMESTAMP"

# ================= READ STREAM FROM LANDING =================
landing_df = spark.readStream \
    .format("parquet") \
    .schema(landing_schema) \
    .option("path", r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\landing\ipl2026") \
    .load()

# ================= TRANSFORM =================
bronze_df = landing_df \
    .withColumn("payload", from_json(col("value"), ipl_schema)) \
    .select(
        # ── Kafka metadata (kept for lineage) ──────────────────────────
        col("key"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),

        # ── Parsed & flattened IPL match fields ────────────────────────
        col("payload.match_id"),
        col("payload.team1"),
        col("payload.team2"),
        col("payload.stadium"),
        col("payload.tickets_sold"),
        col("payload.price"),
        col("payload.timestamp").cast("timestamp").alias("match_timestamp"),

        # ── Bronze audit column ────────────────────────────────────────
        current_timestamp().alias("ingested_at")
    )

# ================= WRITE STREAM TO BRONZE =================
query = bronze_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\bronze\ipl2026") \
    .option("checkpointLocation", r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\checkpoints\bronze\ipl2026") \
    .trigger(processingTime="10 seconds") \
    .start()

# ================= GRACEFUL STOP =================
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping IPL bronze stream gracefully...")
    query.stop()
    spark.stop()
    print("Bronze stream stopped.")