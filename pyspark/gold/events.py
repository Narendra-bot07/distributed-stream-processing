import os
import sys

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg,
    round as spark_round, current_timestamp, desc
)

spark = SparkSession.builder \
    .appName("Gold_Events") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.local.dir", "C:/tmp/spark-temp") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

silver_schema = """
    event_id            STRING,
    event_type          STRING,
    artist              STRING,
    location            STRING,
    tickets             INT,
    price               INT,
    total_revenue       INT,
    event_timestamp     TIMESTAMP,
    event_year          INT,
    event_month         INT,
    event_day           INT,
    event_hour          INT,
    kafka_timestamp     TIMESTAMP,
    bronze_ingested_at  TIMESTAMP,
    silver_ingested_at  TIMESTAMP
"""

silver_df = spark.readStream \
    .format("parquet") \
    .schema(silver_schema) \
    .option("path", r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\silver\events") \
    .load()

def write_gold_tables(batch_df, batch_id):

    batch_df.cache()

    if batch_df.count() == 0:
        print(f"[Batch {batch_id}] Empty batch, skipping.")
        batch_df.unpersist()
        return

    print(f"\n[Batch {batch_id}] Processing {batch_df.count()} rows...")

    # ── GOLD AGG 1: Revenue per Artist ─────────────────────────────
    gold_revenue_per_artist = batch_df \
        .groupBy("artist") \
        .agg(
            spark_sum("total_revenue").alias("total_revenue"),
            count("event_id").alias("total_events"),
            spark_round(avg("price"), 2).alias("avg_ticket_price"),
            spark_sum("tickets").alias("total_tickets_sold")
        ) \
        .withColumn("gold_updated_at", current_timestamp()) \
        .orderBy(desc("total_revenue"))

    gold_revenue_per_artist.write \
        .mode("overwrite") \
        .parquet(r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\gold\events\revenue_per_artist")

    # ── GOLD AGG 2: Revenue per Event Type ─────────────────────────
    gold_revenue_per_event_type = batch_df \
        .groupBy("event_type") \
        .agg(
            spark_sum("total_revenue").alias("total_revenue"),
            count("event_id").alias("total_events"),
            spark_round(avg("total_revenue"), 2).alias("avg_event_revenue"),
            spark_sum("tickets").alias("total_tickets_sold")
        ) \
        .withColumn("gold_updated_at", current_timestamp()) \
        .orderBy(desc("total_revenue"))

    gold_revenue_per_event_type.write \
        .mode("overwrite") \
        .parquet(r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\gold\events\revenue_per_event_type")

    # ── GOLD AGG 3: Tickets Sold per Location ──────────────────────
    gold_tickets_per_location = batch_df \
        .groupBy("location") \
        .agg(
            spark_sum("tickets").alias("total_tickets_sold"),
            count("event_id").alias("total_events"),
            spark_round(avg("tickets"), 2).alias("avg_tickets_per_event"),
            spark_sum("total_revenue").alias("total_revenue")
        ) \
        .withColumn("gold_updated_at", current_timestamp()) \
        .orderBy(desc("total_tickets_sold"))

    gold_tickets_per_location.write \
        .mode("overwrite") \
        .parquet(r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\gold\events\tickets_per_location")

    # ── GOLD AGG 4: Events by Hour ─────────────────────────────────
    gold_events_by_hour = batch_df \
        .groupBy("event_hour") \
        .agg(
            count("event_id").alias("total_events"),
            spark_sum("total_revenue").alias("total_revenue"),
            spark_round(avg("total_revenue"), 2).alias("avg_revenue_per_event")
        ) \
        .withColumn("gold_updated_at", current_timestamp()) \
        .orderBy("event_hour")

    gold_events_by_hour.write \
        .mode("overwrite") \
        .parquet(r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\gold\events\events_by_hour")

    batch_df.unpersist()

    # ── DISPLAY ────────────────────────────────────────────────────
    print("\n" + "="*60)
    print(f"  EVENTS GOLD TABLES — Batch {batch_id}")
    print("="*60)

    print("\n🎤  REVENUE PER ARTIST")
    gold_revenue_per_artist.show(truncate=False)

    print("\n🎪  REVENUE PER EVENT TYPE")
    gold_revenue_per_event_type.show(truncate=False)

    print("\n📍  TICKETS SOLD PER LOCATION")
    gold_tickets_per_location.show(10, truncate=False)

    print("\n⏰  EVENTS BY HOUR")
    gold_events_by_hour.show(24, truncate=False)

    print("="*60)
    print(f"[Batch {batch_id}] Done.")
    print("="*60 + "\n")

query = silver_df.writeStream \
    .outputMode("append") \
    .option("checkpointLocation", r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\checkpoints\gold\events") \
    .trigger(processingTime="20 seconds") \
    .foreachBatch(write_gold_tables) \
    .start()

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping events gold stream...")
    query.stop()
    spark.stop()
    print("Events gold stream stopped.")