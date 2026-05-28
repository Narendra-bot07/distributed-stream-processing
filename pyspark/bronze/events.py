import os
import sys

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("Bronze_Events") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.local.dir", "C:/tmp/spark-temp") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# event_id, event_type, artist, location, tickets, price, timestamp
event_schema = StructType([
    StructField("event_id",   StringType(),  True),
    StructField("event_type", StringType(),  True),
    StructField("artist",     StringType(),  True),
    StructField("location",   StringType(),  True),
    StructField("tickets",    IntegerType(), True),
    StructField("price",      IntegerType(), True),
    StructField("timestamp",  StringType(),  True)
])

landing_schema = "key STRING, value STRING, topic STRING, " \
                 "partition INT, offset LONG, timestamp TIMESTAMP"

landing_df = spark.readStream \
    .format("parquet") \
    .schema(landing_schema) \
    .option("path", r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\landing\events") \
    .load()

bronze_df = landing_df \
    .withColumn("payload", from_json(col("value"), event_schema)) \
    .select(
        col("key"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("payload.event_id"),
        col("payload.event_type"),
        col("payload.artist"),
        col("payload.location"),
        col("payload.tickets"),
        col("payload.price"),
        col("payload.timestamp").cast("timestamp").alias("event_timestamp"),
        current_timestamp().alias("ingested_at")
    )

query = bronze_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\bronze\events") \
    .option("checkpointLocation", r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\checkpoints\bronze\events") \
    .trigger(processingTime="10 seconds") \
    .start()

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping events bronze stream...")
    query.stop()
    spark.stop()
    print("Bronze stream stopped.")