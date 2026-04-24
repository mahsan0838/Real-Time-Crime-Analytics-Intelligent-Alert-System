from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("CrimeAnalytics").getOrCreate()

# Load CSV without schema - let Spark infer but handle properly
crime_df = spark.read.option("header", "true").csv("../data/Crimes_Sample_50k_clean.csv")

# Show actual column names
print("Actual columns in CSV:")
print(crime_df.columns[:10])

# Rename columns to remove spaces for easier handling
for col_name in crime_df.columns:
    new_name = col_name.replace(" ", "_")
    crime_df = crime_df.withColumnRenamed(col_name, new_name)

print("\n=== Top 10 Crime Types (by Primary_Type) ===")
crime_df.groupBy("Primary_Type").count().orderBy(col("count").desc()).show(10)

print("\n=== Arrest Rates by Crime Type ===")
arrest_stats = crime_df.groupBy("Primary_Type").agg(
    count("*").alias("total"),
    sum(when(col("Arrest") == "true", 1).otherwise(0)).alias("arrests")
).withColumn("arrest_rate", round(col("arrests")/col("total")*100, 2))

arrest_stats.filter(col("total") > 100).orderBy(col("arrest_rate").desc()).show(10)

print("\n=== Crime Counts by District ===")
crime_df.groupBy("District").count().orderBy(col("count").desc()).show(15)

print("\n=== Arrest Rate by District ===")
district_stats = crime_df.groupBy("District").agg(
    count("*").alias("total_crimes"),
    sum(when(col("Arrest") == "true", 1).otherwise(0)).alias("total_arrests")
).withColumn("arrest_rate", round(col("total_arrests")/col("total_crimes")*100, 2))

district_stats.orderBy(col("arrest_rate").desc()).show(15)

spark.stop()