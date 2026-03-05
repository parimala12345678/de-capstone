import os

os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-11.0.30.7-hotspot"
os.environ["HADOOP_HOME"] = r"C:\pyspark\hadoop"
os.environ["PATH"] += r";C:\pyspark\hadoop\bin"

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Test") \
    .master("local[*]") \
    .getOrCreate()

print("Spark started successfully!")

spark.stop()