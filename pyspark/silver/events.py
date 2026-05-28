import os
import sys

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

spark = SparkSession.builder \
    .appName("Silver_Events") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.local.dir", "C:/tmp/spark-temp") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

bronze_schema = """
    key               STRING,
    topic             STRING,
    partition         INT,
    offset            LONG,
    kafka_timestamp   TIMESTAMP,
    event_id          STRING,
    event_type        STRING,
    artist            STRING,
    location          STRING,
    tickets           INT,
    price             INT,
    event_timestamp   TIMESTAMP,
    ingested_at       TIMESTAMP
"""

bronze_df = spark.readStream \
    .format("parquet") \
    .schema(bronze_schema) \
    .option("path", r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\bronze\events") \
    .load()

silver_df = bronze_df \
    .filter(col("event_id").isNotNull()) \
    .filter(col("event_type").isNotNull()) \
    .filter(col("artist").isNotNull()) \
    .filter(col("location").isNotNull()) \
    .filter(col("tickets") > 0) \
    .filter(col("price") > 0) \
    \
    .withColumn("event_type", initcap(trim(col("event_type")))) \
    .withColumn("artist",     initcap(trim(col("artist")))) \
    .withColumn("location",   initcap(trim(col("location")))) \
    \
    .withColumn("total_revenue", spark_round(col("tickets") * col("price"), 2)) \
    \
    .withColumn("event_year",  year(col("event_timestamp"))) \
    .withColumn("event_month", month(col("event_timestamp"))) \
    .withColumn("event_day",   dayofmonth(col("event_timestamp"))) \
    .withColumn("event_hour",  hour(col("event_timestamp"))) \
    \
    .select(
        col("event_id"),
        col("event_type"),
        col("artist"),
        col("location"),
        col("tickets"),
        col("price"),
        col("total_revenue"),
        col("event_timestamp"),
        col("event_year"),
        col("event_month"),
        col("event_day"),
        col("event_hour"),
        col("kafka_timestamp"),
        col("ingested_at").alias("bronze_ingested_at"),
        current_timestamp().alias("silver_ingested_at")
    )

query = silver_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\silver\events") \
    .option("checkpointLocation", r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\checkpoints\silver\events") \
    .trigger(processingTime="15 seconds") \
    .start()

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping events silver stream...")
    query.stop()
    spark.stop()
    print("Silver stream stopped.")