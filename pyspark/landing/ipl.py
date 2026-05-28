import os
import sys

# ================= WINDOWS FIX (must be before SparkSession) =================
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

# ================= SPARK SESSION =================
spark = SparkSession.builder \
    .appName("Kafka_IPL_Landing") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.local.dir", "C:/tmp/spark-temp") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ================= KAFKA CONFIG =================
kafka_conf = {
    "kafka.bootstrap.servers": "localhost:9092",
    "subscribe": "ipl2026",
    "startingOffsets": "earliest",
    "failOnDataLoss": "false"
}

# ================= READ STREAM =================
raw_df = spark.readStream \
    .format("kafka") \
    .options(**kafka_conf) \
    .load()

# ================= TRANSFORM =================
landing_df = raw_df.selectExpr(
    "CAST(key AS STRING) as key",
    "CAST(value AS STRING) as value",
    "topic",
    "partition",
    "offset",
    "timestamp"
)

# ================= WRITE STREAM =================
query = landing_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\landing\ipl2026") \
    .option("checkpointLocation", r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\checkpoints\landing\ipl2026") \
    .trigger(processingTime="5 seconds") \
    .start()

# ================= GRACEFUL STOP =================
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping IPL landing stream gracefully...")
    query.stop()
    spark.stop()
    print("Stream stopped.")