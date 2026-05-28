import os
import sys

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

gold_base = r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\gold"

paths = {
    "Revenue per Movie" : rf"{gold_base}\revenue_per_movie",
    "Revenue per City"  : rf"{gold_base}\revenue_per_city",
    "Seats per Movie"   : rf"{gold_base}\seats_per_movie",
    "Bookings by Hour"  : rf"{gold_base}\bookings_by_hour",
}

print("[DEBUG] Checking gold layer paths...\n")
for name, path in paths.items():
    if not os.path.exists(path):
        print(f"[DEBUG] ERROR: '{name}' path does not exist: {path}")
        print("[DEBUG] Run the gold writer first.")
        sys.exit(1)
    files = [f for f in os.listdir(path) if f.endswith(".parquet")]
    print(f"[DEBUG] OK ({len(files)} parquet files) : {name}")

spark = SparkSession.builder \
    .appName("Read_Gold_Movies") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark._jvm.org.apache.log4j.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager").setLevel(spark._jvm.org.apache.log4j.Level.OFF)
print("\n[DEBUG] Spark session created.\n")

for name, path in paths.items():
    print("=" * 60)
    print(f"  GOLD — {name.upper()}")
    print("=" * 60)
    df = spark.read.format("parquet").load(path)
    print("[DEBUG] Schema:")
    df.printSchema()
    print(f"[DEBUG] Total rows: {df.count()}")
    df.show(truncate=False)
    print()

spark.stop()
print("[DEBUG] Spark stopped.")