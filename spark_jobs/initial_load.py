import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    year, month, to_timestamp, col, split, explode, trim
)

RAW = os.environ["DATA_RAW"]
PROCESSED = os.environ["DATA_PROCESSED"]

spark = SparkSession.builder \
    .appName("mta-initial-load") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# -- Ridership 2025 (by station_complex)
df = spark.read.parquet(f"{RAW}/ridership_2025.parquet", header=True, inferSchema=True)
df = df.withColumn("transit_timestamp", to_timestamp("transit_timestamp"))
df = df.withColumn("year", year("transit_timestamp")) \
       .withColumn("month", month("transit_timestamp"))

df.write.mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(f"{PROCESSED}/ridership/")

# -- APT 2025 dataset (by subway line)
apt = spark.read.csv(f"{RAW}/apt_2025.csv", header=True, inferSchema=True)
apt = apt.withColumn("month", to_timestamp("month"))
apt = apt.withColumn("year", year("month")) \
         .withColumn("month_num", month("month"))
apt.write.mode("overwrite").parquet(f"{PROCESSED}/apt/")

# -- Station reference: build the complex-to-route bridge table
stations = spark.read.csv(f"{RAW}/stations_reference.csv", header=True, inferSchema=True)

station_routes = stations.select(
    col("complex_id").alias("complex_id"),
    col("stop_name").alias("stop_name"),
    col("division").alias("division"),
    col("borough").alias("borough_code"),
    col("daytime_routes").alias("daytime_routes"),
)

# Explode into separate rows, one per route
station_routes = station_routes \
    .withColumn("route", explode(split(col("daytime_routes"), " "))) \
    .withColumn("route", trim(col("route"))) \
    .filter(col("route") != "")

# Deduplicate: multiple physical stations in the same complex may list
# the same route. We only need one mapping per (complex_id, route).
station_routes = station_routes \
    .select("complex_id", "route", "division", "borough_code") \
    .distinct()

station_routes.write.mode("overwrite").parquet(f"{PROCESSED}/station_route_lookup/")

# Also persist a station_name_check for the data quality checks
station_name_check = df.select("station_complex", "station_complex_id", "borough") \
    .distinct()
station_name_check.write.mode("overwrite").parquet(f"{PROCESSED}/station_name_check")

spark.stop()