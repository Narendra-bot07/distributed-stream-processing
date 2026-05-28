import os
import sys

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Landing_Events") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.local.dir", "C:/tmp/spark-temp") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

kafka_conf = {
    "kafka.bootstrap.servers": "localhost:9092",
    "subscribe": "events",
    "startingOffsets": "earliest",
    "failOnDataLoss": "false"
}

raw_df = spark.readStream \
    .format("kafka") \
    .options(**kafka_conf) \
    .load()

landing_df = raw_df.selectExpr(
    "CAST(key AS STRING) as key",
    "CAST(value AS STRING) as value",
    "topic",
    "partition",
    "offset",
    "timestamp"
)

query = landing_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\landing\events") \
    .option("checkpointLocation", r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\checkpoints\landing\events") \
    .trigger(processingTime="5 seconds") \
    .start()

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping events landing stream...")
    query.stop()
    spark.stop()
    print("Stream stopped.")