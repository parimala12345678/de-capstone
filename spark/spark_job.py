import os

os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
os.environ["PATH"] = os.environ["JAVA_HOME"] + r"\bin;" + os.environ["PATH"]

from pyspark.sql import SparkSession



from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Trip Analysis") \
    .getOrCreate()

# Read CSV
df = spark.read.csv(
    "input.csv",
    header=True,
    inferSchema=True
)

# Convert timestamps
df = df.withColumn(
    "pickup_time",
    to_timestamp("pickup_time")
).withColumn(
    "dropoff_time",
    to_timestamp("dropoff_time")
)

# Trip duration
df = df.withColumn(
    "trip_duration_minutes",
    (col("dropoff_time").cast("long") -
     col("pickup_time").cast("long")) / 60
)

# Aggregation
daily_avg = df.groupBy(
    to_date("pickup_time").alias("trip_date")
).agg(
    avg("trip_duration_minutes").alias("avg_duration")
)

# Write parquet
daily_avg.write.mode("overwrite").parquet("spark_output")

spark.stop()