from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, when, year, month, hour, to_timestamp, date_format
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.runtime import get_postgres, load_config, resolve_data_path

spark = SparkSession.builder.appName("PopulateTables").getOrCreate()

# Load config
CFG = load_config()
PG = get_postgres(CFG)

# Load crime data
crime_path = str(resolve_data_path(CFG, "crime"))
crime_df = spark.read.option("header", "true").csv(crime_path)
for old in crime_df.columns:
    crime_df = crime_df.withColumnRenamed(old, old.replace(" ", "_"))

# 1. Crime Trends by Year, Month, Day of Week
print("Calculating crime trends...")
crime_trends = crime_df \
    .withColumn("year", year(to_timestamp("Date", "MM/dd/yyyy hh:mm:ss a"))) \
    .withColumn("month", month(to_timestamp("Date", "MM/dd/yyyy hh:mm:ss a"))) \
    .withColumn("day_of_week", date_format(to_timestamp("Date", "MM/dd/yyyy hh:mm:ss a"), "EEEE")) \
    .groupBy("year", "month", "day_of_week") \
    .count() \
    .withColumnRenamed("count", "crime_count")

crime_trends.write.format("jdbc").mode("overwrite").options(
    url=f"jdbc:postgresql://{PG.host}:{PG.port}/{PG.database}",
    driver="org.postgresql.Driver",
    dbtable="crime_trends",
    user=PG.user,
    password=PG.password,
).save()
print("Crime trends saved to PostgreSQL")

# 2. Arrest Rates by Crime Type
print("Calculating arrest rates...")
arrest_rates = crime_df.groupBy("Primary_Type").agg(
    count("*").alias("total_crimes"),
    sum(when(col("Arrest") == "true", 1).otherwise(0)).alias("total_arrests")
).withColumn("arrest_rate", ((col("total_arrests") / col("total_crimes")) * 100).cast("decimal(10,2)"))

arrest_rates = arrest_rates.withColumnRenamed("Primary_Type", "crime_type")

arrest_rates.write.format("jdbc").mode("overwrite").options(
    url=f"jdbc:postgresql://{PG.host}:{PG.port}/{PG.database}",
    driver="org.postgresql.Driver",
    dbtable="arrest_rates",
    user=PG.user,
    password=PG.password,
).save()
print("Arrest rates saved to PostgreSQL")

spark.stop()
print("✅ Done!")