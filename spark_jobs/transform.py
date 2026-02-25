import argparse, os
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

PROCESSED = os.environ["DATA_PROCESSED"]

parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--build-baseline", action="store_true",
                    help="Compute and persist the 2025 CSI baseline. Run once after initial load.")

args = parser.parse_args()

spark = SparkSession.builder \
    .appName("mta-transform") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

df = spark.read.parquet(args.input)
apt = spark.read.parquet(f"{PROCESSED}/apt/")


# set transit_timestamp column data type to timestamp
df = df.withColumn("transit_timestamp", F.to_timestamp("transit_timestamp"))

# aggregate ridership per station-hour (combine MetroCard + OMNY)
df = df.groupBy("station_complex", "transit_timestamp").agg(
    F.sum("ridership").alias("ridership")
)

# add columns for additional time features based on the timestamp
df = df \
    .withColumn("hour_of_day", F.hour("transit_timestamp")) \
    .withColumn("day_of_week",  F.dayofweek("transit_timestamp")) \
    .withColumn("is_weekend", (F.dayofweek("transit_timestamp").isin(1,7).cast("int"))) \
    .withColumn("month",        F.month("transit_timestamp")) \
    .withColumn("year",         F.year("transit_timestamp")) \
    .withColumn("time_window",
                F.when(F.col("hour_of_day").between(0,6), "early_morning")
                .when(F.col("hour_of_day").between(7,9), "morning_peak")
                .when(F.col("hour_of_day").between(10,15), "midday")
                .when(F.col("hour_of_day").between(16,19), "evening_peak")
                .otherwise("late_night"))