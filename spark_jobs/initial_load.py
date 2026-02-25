import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, to_timestamp

RAW = os.environ["DATA_RAW"]
PROCESSED = os.environ["DATA_PROCESSED"]

spark = SparkSession.builder \
    .appName("mta-initial-load") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# -- Ridership 2025
df = spark.read.parquet(f"{RAW}/ridership_2025.parquet", header=True, inferSchema=True)
df = df.withColumn("transit_timestamp", to_timestamp("transit_timestamp"))
df = df.withColumn("year", year("transit_timestamp")) \
       .withColumn("month", month("transit_timestamp"))

df.write.mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(f"{PROCESSED}/ridership/")

# -- APT 2025 dataset
apt = spark.read.csv(f"{RAW}/apt_2025.csv", header=True, inferSchema=True)
apt.write.mode("overwrite").parquet(f"{PROCESSED}/apt/")

spark.stop()