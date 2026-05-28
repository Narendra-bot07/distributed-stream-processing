import os
import sys

# ================= WINDOWS FIX =================
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg,
    round as spark_round, current_timestamp, desc
)

# ================= SPARK SESSION =================
spark = SparkSession.builder \
    .appName("Gold_IPL2026") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.local.dir", "C:/tmp/spark-temp") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ================= SILVER SCHEMA =================
silver_schema = """
    match_id            STRING,
    team1               STRING,
    team2               STRING,
    stadium             STRING,
    tickets_sold        INT,
    price               INT,
    total_revenue       INT,
    match_timestamp     TIMESTAMP,
    match_year          INT,
    match_month         INT,
    match_day           INT,
    match_hour          INT,
    kafka_timestamp     TIMESTAMP,
    bronze_ingested_at  TIMESTAMP,
    silver_ingested_at  TIMESTAMP
"""

# ================= READ STREAM FROM SILVER =================
silver_df = spark.readStream \
    .format("parquet") \
    .schema(silver_schema) \
    .option("path", r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\silver\ipl2026") \
    .load()

# ================= foreachBatch HANDLER =================
def write_gold_tables(batch_df, batch_id):

    batch_df.cache()

    if batch_df.count() == 0:
        print(f"[Batch {batch_id}] Empty batch, skipping.")
        batch_df.unpersist()
        return

    print(f"\n[Batch {batch_id}] Processing {batch_df.count()} rows...")

    # ── GOLD AGG 1: Revenue per Team ───────────────────────────────
    # each match has team1 & team2 — groupBy team1 shows revenue
    # generated when that team is the home/first listed side
    gold_revenue_per_team = batch_df \
        .groupBy("team1") \
        .agg(
            spark_sum("total_revenue").alias("total_revenue"),
            count("match_id").alias("total_matches"),
            spark_round(avg("price"), 2).alias("avg_ticket_price"),
            spark_sum("tickets_sold").alias("total_tickets_sold")
        ) \
        .withColumn("gold_updated_at", current_timestamp()) \
        .orderBy(desc("total_revenue"))

    gold_revenue_per_team.write \
        .mode("overwrite") \
        .parquet(r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\gold\ipl\revenue_per_team")

    # ── GOLD AGG 2: Revenue per Stadium ────────────────────────────
    gold_revenue_per_stadium = batch_df \
        .groupBy("stadium") \
        .agg(
            spark_sum("total_revenue").alias("total_revenue"),
            count("match_id").alias("total_matches"),
            spark_round(avg("total_revenue"), 2).alias("avg_match_revenue"),
            spark_sum("tickets_sold").alias("total_tickets_sold")
        ) \
        .withColumn("gold_updated_at", current_timestamp()) \
        .orderBy(desc("total_revenue"))

    gold_revenue_per_stadium.write \
        .mode("overwrite") \
        .parquet(r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\gold\ipl\revenue_per_stadium")

    # ── GOLD AGG 3: Tickets Sold per Team ──────────────────────────
    gold_tickets_per_team = batch_df \
        .groupBy("team1") \
        .agg(
            spark_sum("tickets_sold").alias("total_tickets_sold"),
            count("match_id").alias("total_matches"),
            spark_round(avg("tickets_sold"), 2).alias("avg_tickets_per_match")
        ) \
        .withColumn("gold_updated_at", current_timestamp()) \
        .orderBy(desc("total_tickets_sold"))

    gold_tickets_per_team.write \
        .mode("overwrite") \
        .parquet(r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\gold\ipl\tickets_per_team")

    # ── GOLD AGG 4: Matches by Hour ────────────────────────────────
    gold_matches_by_hour = batch_df \
        .groupBy("match_hour") \
        .agg(
            count("match_id").alias("total_matches"),
            spark_sum("total_revenue").alias("total_revenue"),
            spark_round(avg("total_revenue"), 2).alias("avg_revenue_per_match")
        ) \
        .withColumn("gold_updated_at", current_timestamp()) \
        .orderBy("match_hour")

    gold_matches_by_hour.write \
        .mode("overwrite") \
        .parquet(r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\gold\ipl\matches_by_hour")

    batch_df.unpersist()

    # ── DISPLAY ────────────────────────────────────────────────────
    print("\n" + "="*60)
    print(f"  IPL GOLD TABLES — Batch {batch_id}")
    print("="*60)

    print("\n🏏  REVENUE PER TEAM")
    gold_revenue_per_team.show(truncate=False)

    print("\n🏟️  REVENUE PER STADIUM")
    gold_revenue_per_stadium.show(truncate=False)

    print("\n🎟️  TICKETS SOLD PER TEAM")
    gold_tickets_per_team.show(truncate=False)

    print("\n⏰  MATCHES BY HOUR")
    gold_matches_by_hour.show(24, truncate=False)

    print("="*60)
    print(f"[Batch {batch_id}] Done.")
    print("="*60 + "\n")

# ================= SINGLE STREAM WITH foreachBatch =================
query = silver_df.writeStream \
    .outputMode("append") \
    .option("checkpointLocation", r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\checkpoints\gold\ipl2026") \
    .trigger(processingTime="20 seconds") \
    .foreachBatch(write_gold_tables) \
    .start()

# ================= GRACEFUL STOP =================
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping IPL gold stream...")
    query.stop()
    spark.stop()
    print("IPL gold stream stopped.")