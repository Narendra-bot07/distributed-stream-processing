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
    .appName("Silver_IPL2026") \
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
    match_id          STRING,
    team1             STRING,
    team2             STRING,
    stadium           STRING,
    tickets_sold      INT,
    price             INT,
    match_timestamp   TIMESTAMP,
    ingested_at       TIMESTAMP
"""

bronze_df = spark.readStream \
    .format("parquet") \
    .schema(bronze_schema) \
    .option("path", r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\bronze\ipl2026") \
    .load()

silver_df = bronze_df \
    \
    .filter(col("match_id").isNotNull()) \
    .filter(col("team1").isNotNull()) \
    .filter(col("team2").isNotNull()) \
    .filter(col("stadium").isNotNull()) \
    .filter(col("tickets_sold") > 0) \
    .filter(col("price") > 0) \
    \
    .withColumn("team1",   initcap(trim(col("team1")))) \
    .withColumn("team2",   initcap(trim(col("team2")))) \
    .withColumn("stadium", initcap(trim(col("stadium")))) \
    \
    .withColumn("total_revenue", spark_round(col("tickets_sold") * col("price"), 2)) \
    \
    .withColumn("match_year",  year(col("match_timestamp"))) \
    .withColumn("match_month", month(col("match_timestamp"))) \
    .withColumn("match_day",   dayofmonth(col("match_timestamp"))) \
    .withColumn("match_hour",  hour(col("match_timestamp"))) \
    \
    .select(
        col("match_id"),
        col("team1"),
        col("team2"),
        col("stadium"),
        col("tickets_sold"),
        col("price"),
        col("total_revenue"),

        col("match_timestamp"),
        col("match_year"),
        col("match_month"),
        col("match_day"),
        col("match_hour"),

        col("kafka_timestamp"),
        col("ingested_at").alias("bronze_ingested_at"),
        current_timestamp().alias("silver_ingested_at")
    )

query = silver_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\silver\ipl2026") \
    .option("checkpointLocation", r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\checkpoints\silver\ipl2026") \
    .trigger(processingTime="15 seconds") \
    .start()

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping IPL silver stream gracefully...")
    query.stop()
    spark.stop()
    print("Silver stream stopped.")