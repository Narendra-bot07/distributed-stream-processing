import os
import sys
import shutil

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

checkpoint_path = r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\checkpoints\movies"
landing_path    = r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\landing\movies"

# ── Clear stale checkpoint ───────────────────────────────────────────────────
if os.path.exists(checkpoint_path):
    shutil.rmtree(checkpoint_path)
    print(f"[DEBUG] Cleared stale checkpoint: {checkpoint_path}")
else:
    print(f"[DEBUG] No stale checkpoint found at: {checkpoint_path}")

spark = SparkSession.builder \
    .appName("Kafka_Movies_Local") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("[DEBUG] Spark session created successfully")

kafka_conf = {
    "kafka.bootstrap.servers": "localhost:9092",
    "subscribe": "movies",
    "startingOffsets": "earliest",
    "failOnDataLoss": "false"
}

print("[DEBUG] Connecting to Kafka topic 'movies' at localhost:9092 ...")
raw_df = spark.readStream \
    .format("kafka") \
    .options(**kafka_conf) \
    .load()

print("[DEBUG] Kafka readStream created. Schema:")
raw_df.printSchema()

landing_df = raw_df.selectExpr(
    "CAST(key AS STRING) as key",
    "CAST(value AS STRING) as value",
    "topic",
    "partition",
    "offset",
    "timestamp"
)

print("[DEBUG] Projection applied. Final schema:")
landing_df.printSchema()

print(f"[DEBUG] Writing to landing path : {landing_path}")
print(f"[DEBUG] Checkpoint path         : {checkpoint_path}")

query = landing_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", landing_path) \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(processingTime="5 seconds") \
    .start()

print("[DEBUG] Stream started. Waiting for data every 5 seconds ...")
print("[DEBUG] Press Ctrl+C to stop.\n")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n[DEBUG] KeyboardInterrupt received.")
    print("[DEBUG] Stopping stream gracefully...")
    query.stop()
    spark.stop()
    print("[DEBUG] Stream and Spark stopped.")
    print(f"[DEBUG] Check output at: {landing_path}")