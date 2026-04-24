from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("CrimeAnalytics").getOrCreate()

# Schema with correct column names matching CSV
crime_schema = StructType([
    StructField("Case Number", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Block", StringType(), True),
    StructField("Primary Type", StringType(), True),
    StructField("District", StringType(), True),
    StructField("Arrest", StringType(), True),
    StructField("Latitude", StringType(), True),
    StructField("Longitude", StringType(), True)
])

# Load data
crime_df = spark.read.option("header", "true").schema(crime_schema).csv("../data/Crimes_Sample_50k_clean.csv")
print(f"Loaded {crime_df.count()} crime records")

# Basic stats - no date parsing
print("\n=== Top 10 Crime Types ===")
crime_df.groupBy("Primary Type").count().orderBy(col("count").desc()).show(10)

# Arrest rates by crime type
print("\n=== Top 10 Arrest Rates by Crime Type ===")
arrest_stats = crime_df.groupBy("Primary Type").agg(
    count("*").alias("total"),
    sum(when(col("Arrest") == "true", 1).otherwise(0)).alias("arrests")
).withColumn("arrest_rate", round(col("arrests")/col("total")*100, 2))

arrest_stats.filter(col("total") > 50).orderBy(col("arrest_rate").desc()).show(10)

# Arrest rates by district
print("\n=== Arrest Rates by District ===")
district_stats = crime_df.groupBy("District").agg(
    count("*").alias("total"),
    sum(when(col("Arrest") == "true", 1).otherwise(0)).alias("arrests")
).withColumn("arrest_rate", round(col("arrests")/col("total")*100, 2))

district_stats.orderBy("District").show(20)

# Crime counts by district
print("\n=== Crime Counts by District ===")
crime_df.groupBy("District").count().orderBy(col("count").desc()).show(20)

spark.stop()