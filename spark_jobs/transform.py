import argparse
import os
import json
from pathlib import Path

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

# define a window for summing up ridership for each station-month
station_month_w = Window.partitionBy("station_complex", "year", "month")
df = df.withColumn("monthly_ridership", F.sum("ridership").over(station_month_w))

# rank monthly ridership among all stations
rank_w = Window.partitionBy("year", "month").orderBy(F.desc("monthly_ridership"))
df = df.withColumn("monthly_rank", F.dense_rank(rank_w))

# focus in on sum of ridership volume in morning and evening peak hours
peak_df = df.filter(F.col("time_window").isin("morning_peak", "evening_peak")) \
    .groupBy("station_complex", "year", "month") \
    .agg(F.sum("ridership").alias("peak_hr_ridership"))

# compute a baseline volume for detecting unusually high ridership volume 
# in a station (> 90th percentile of system-wide monthly peak-hours ridership)
baseline_path = Path(PROCESSED) / "csi_baseline" / "p90_peak_hr_ridership.json"

if args.build_baseline:
    p90 = peak_df.approxQuantile("peak_hr_ridership", [0.9], 0.01)[0]
    baseline_path.parent.mkdir(parents=True, exist_ok=True)
    baseline_path.write_text(json.dumps({"p90_peak_hr_ridership": p90}))
    print(f"2025 baseline peak-hr monthly ridership (90th percentile) = {p90:,.0f}")
else:
    p90 = json.loads(baseline_path.read_text())["p90_peak_hr_ridership"]

peak_df = peak_df.withColumn("volume_component", F.col("peak_hr_ridership") / F.lit(p90))

# compute HHI of hourly ridership for each station-month 
# for detecting when station experiences concentrated volume 
# of ridership within a shorter time frame within that month
monthly_total_w = Window.partitionBy("station_complex", "year", "month")
df_hhi = df \
    .withColumn("monthly_total", F.sum("ridership").over(monthly_total_w)) \
    .withColumn("monthly_hour_share_sq", (F.col("ridership") / F.col("monthly_total")) ** 2) \
    .groupBy("station_complex", "year", "month") \
    .agg(F.sum("monthly_hour_share_sq").alias("hhi_monthly_concentration"))

# compute capacity stress index (CSI) as a product of normalized volume and HHI
csi_df = peak_df.join(df_hhi, ["station_complex", "year", "month"]) \
    .withColumn("csi", F.col("volume_component") * F.col("hhi_monthly_concentration"))