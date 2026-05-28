import os
import sys

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

bronze_path = r"D:\PICTURES\OneDrive\Desktop\Kafka\pyspark\data\bronze\movies"

if not os.path.exists(bronze_path):
    print(f"[DEBUG] ERROR: Bronze path does not exist: {bronze_path}")
    print("[DEBUG] Run the bronze writer first.")
    sys.exit(1)

files = [f for f in os.listdir(bronze_path) if f.endswith(".parquet")]
print(f"[DEBUG] Bronze path exists. Parquet files found: {len(files)}")
if not files:
    print("[DEBUG] ERROR: No parquet files found. Nothing to read.")
    sys.exit(1)

spark = SparkSession.builder \
    .appName("Read_Bronze_Movies") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark._jvm.org.apache.log4j.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager").setLevel(spark._jvm.org.apache.log4j.Level.OFF)
print("[DEBUG] Spark session created.")

df = spark.read.format("parquet").load(bronze_path)

print("[DEBUG] Schema:")
df.printSchema()
print(f"[DEBUG] Total rows: {df.count()}")
print("\n--- Bronze Layer Data ---")
df.show(truncate=False)

spark.stop()
print("[DEBUG] Spark stopped.")