import os
import sys
import shutil

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg,
    round as spark_round, current_timestamp, desc
)

silver_path     = r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\silver\movies"
checkpoint_path = r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\checkpoints\gold\movies"

gold_revenue_per_movie_path = r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\gold\revenue_per_movie"
gold_revenue_per_city_path  = r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\gold\revenue_per_city"
gold_seats_per_movie_path   = r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\gold\seats_per_movie"
gold_bookings_by_hour_path  = r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\gold\bookings_by_hour"

# ── Debug: check silver path has data ───────────────────────────────────────
if not os.path.exists(silver_path):
    print(f"[DEBUG] ERROR: Silver path does not exist: {silver_path}")
    print("[DEBUG] Run the silver writer first before starting gold.")
    sys.exit(1)

silver_files = [f for f in os.listdir(silver_path) if f.endswith(".parquet")]
print(f"[DEBUG] Silver path exists. Parquet files found: {len(silver_files)}")
if not silver_files:
    print("[DEBUG] ERROR: No parquet files in silver path. Nothing to read.")
    sys.exit(1)

# ── Clear stale checkpoint ───────────────────────────────────────────────────
if os.path.exists(checkpoint_path):
    shutil.rmtree(checkpoint_path)
    print(f"[DEBUG] Cleared stale checkpoint: {checkpoint_path}")
else:
    print("[DEBUG] No stale checkpoint found.")

spark = SparkSession.builder \
    .appName("Gold_Movies") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("[DEBUG] Spark session created.")

silver_schema = """
    booking_id          STRING,
    user                STRING,
    movie               STRING,
    city                STRING,
    seats               INT,
    price               INT,
    total_amount        INT,
    booking_timestamp   TIMESTAMP,
    booking_year        INT,
    booking_month       INT,
    booking_day         INT,
    booking_hour        INT,
    kafka_timestamp     TIMESTAMP,
    bronze_ingested_at  TIMESTAMP,
    silver_ingested_at  TIMESTAMP
"""

print(f"[DEBUG] Reading stream from silver path: {silver_path}")
silver_df = spark.readStream \
    .format("parquet") \
    .schema(silver_schema) \
    .option("path", silver_path) \
    .load()

print("[DEBUG] Silver readStream created. Schema:")
silver_df.printSchema()

# ── foreachBatch handler ─────────────────────────────────────────────────────
def write_gold_tables(batch_df, batch_id):

    print(f"\n[DEBUG] Batch {batch_id} received.")
    batch_df.cache()

    row_count = batch_df.count()
    print(f"[DEBUG] Batch {batch_id} row count: {row_count}")

    if row_count == 0:
        print(f"[DEBUG] Batch {batch_id} is empty, skipping.")
        batch_df.unpersist()
        return

    print(f"[DEBUG] Batch {batch_id} — computing gold aggregations...")

    # ── GOLD AGG 1: Revenue per Movie ────────────────────────────────
    print("[DEBUG] Computing revenue_per_movie...")
    gold_revenue_per_movie = batch_df \
        .groupBy("movie") \
        .agg(
            spark_sum("total_amount").alias("total_revenue"),
            count("booking_id").alias("total_bookings"),
            spark_round(avg("price"), 2).alias("avg_ticket_price"),
            spark_sum("seats").alias("total_seats_sold")
        ) \
        .withColumn("gold_updated_at", current_timestamp()) \
        .orderBy(desc("total_revenue"))

    gold_revenue_per_movie.write \
        .mode("overwrite") \
        .parquet(gold_revenue_per_movie_path)
    print(f"[DEBUG] revenue_per_movie written to: {gold_revenue_per_movie_path}")

    # ── GOLD AGG 2: Revenue per City ─────────────────────────────────
    print("[DEBUG] Computing revenue_per_city...")
    gold_revenue_per_city = batch_df \
        .groupBy("city") \
        .agg(
            spark_sum("total_amount").alias("total_revenue"),
            count("booking_id").alias("total_bookings"),
            spark_round(avg("total_amount"), 2).alias("avg_booking_value")
        ) \
        .withColumn("gold_updated_at", current_timestamp()) \
        .orderBy(desc("total_revenue"))

    gold_revenue_per_city.write \
        .mode("overwrite") \
        .parquet(gold_revenue_per_city_path)
    print(f"[DEBUG] revenue_per_city written to: {gold_revenue_per_city_path}")

    # ── GOLD AGG 3: Seats Sold per Movie ─────────────────────────────
    print("[DEBUG] Computing seats_per_movie...")
    gold_seats_per_movie = batch_df \
        .groupBy("movie") \
        .agg(
            spark_sum("seats").alias("total_seats_sold"),
            count("booking_id").alias("total_bookings"),
            spark_round(avg("seats"), 2).alias("avg_seats_per_booking")
        ) \
        .withColumn("gold_updated_at", current_timestamp()) \
        .orderBy(desc("total_seats_sold"))

    gold_seats_per_movie.write \
        .mode("overwrite") \
        .parquet(gold_seats_per_movie_path)
    print(f"[DEBUG] seats_per_movie written to: {gold_seats_per_movie_path}")

    # ── GOLD AGG 4: Bookings by Hour ─────────────────────────────────
    print("[DEBUG] Computing bookings_by_hour...")
    gold_bookings_by_hour = batch_df \
        .groupBy("booking_hour") \
        .agg(
            count("booking_id").alias("total_bookings"),
            spark_sum("total_amount").alias("total_revenue"),
            spark_round(avg("total_amount"), 2).alias("avg_revenue_per_booking")
        ) \
        .withColumn("gold_updated_at", current_timestamp()) \
        .orderBy("booking_hour")

    gold_bookings_by_hour.write \
        .mode("overwrite") \
        .parquet(gold_bookings_by_hour_path)
    print(f"[DEBUG] bookings_by_hour written to: {gold_bookings_by_hour_path}")

    batch_df.unpersist()

    # ── Display ───────────────────────────────────────────────────────
    print("\n" + "="*60)
    print(f"  GOLD TABLES — Batch {batch_id}")
    print("="*60)

    print("\n📽️  REVENUE PER MOVIE")
    gold_revenue_per_movie.show(truncate=False)

    print("\n🏙️  REVENUE PER CITY (top 10)")
    gold_revenue_per_city.show(10, truncate=False)

    print("\n🎟️  SEATS SOLD PER MOVIE")
    gold_seats_per_movie.show(truncate=False)

    print("\n⏰  BOOKINGS BY HOUR")
    gold_bookings_by_hour.show(24, truncate=False)

    print("="*60)
    print(f"[DEBUG] Batch {batch_id} done.")
    print("="*60 + "\n")

print(f"[DEBUG] Writing gold tables via foreachBatch every 20 seconds...")
print(f"[DEBUG] Checkpoint path: {checkpoint_path}")
print("[DEBUG] Press Ctrl+C to stop.\n")

query = silver_df.writeStream \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(processingTime="20 seconds") \
    .foreachBatch(write_gold_tables) \
    .start()

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n[DEBUG] KeyboardInterrupt received.")
    print("[DEBUG] Stopping gold stream gracefully...")
    query.stop()
    spark.stop()
    print("[DEBUG] Gold stream stopped.")
    print(f"[DEBUG] Check gold outputs at: D:\\PICTURES\\OneDrive\\Desktop\\Kafka\\pyspark\\data\\gold\\")