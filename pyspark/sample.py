import os
import sys

# ================= WINDOWS FIX =================
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

# ================= SPARK SESSION =================
spark = SparkSession.builder \
    .appName("Read_Gold_Movies") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

BASE = r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\gold"

# ================= READ & DISPLAY ALL 4 GOLD TABLES =================

print("\n" + "="*60)
print("  GOLD LAYER — MOVIES ANALYTICS")
print("="*60)

print("\n📽️  REVENUE PER MOVIE")
spark.read \
    .parquet(f"{BASE}\\revenue_per_movie") \
    .orderBy(desc("total_revenue")) \
    .show(truncate=False)

print("\n🏙️  REVENUE PER CITY (top 10)")
spark.read \
    .parquet(f"{BASE}\\revenue_per_city") \
    .orderBy(desc("total_revenue")) \
    .show(10, truncate=False)

print("\n🎟️  SEATS SOLD PER MOVIE")
spark.read \
    .parquet(f"{BASE}\\seats_per_movie") \
    .orderBy(desc("total_seats_sold")) \
    .show(truncate=False)

print("\n⏰  BOOKINGS BY HOUR")
spark.read \
    .parquet(f"{BASE}\\bookings_by_hour") \
    .orderBy("booking_hour") \
    .show(24, truncate=False)

print("="*60)

# ================= STOP =================
spark.stop()