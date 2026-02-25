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

