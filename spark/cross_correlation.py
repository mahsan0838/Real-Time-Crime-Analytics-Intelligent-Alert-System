from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, avg, corr
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.runtime import load_config, resolve_data_path

spark = SparkSession.builder.appName("CrossCorrelation").getOrCreate()

# Load config
CFG = load_config()

# Load Crime data
crime_df = spark.read.option("header", "true").csv(str(resolve_data_path(CFG, "crime")))
for old in crime_df.columns:
    crime_df = crime_df.withColumnRenamed(old, old.replace(" ", "_"))

# Load Violence data
violence_df = spark.read.option("header", "true").csv(str(resolve_data_path(CFG, "violence")))
for old in violence_df.columns:
    violence_df = violence_df.withColumnRenamed(old, old.replace(" ", "_"))

# Load Sex Offenders data
sex_df = spark.read.option("header", "true").csv(str(resolve_data_path(CFG, "sex_offenders")))
for old in sex_df.columns:
    sex_df = sex_df.withColumnRenamed(old, old.replace(" ", "_"))

# Load Police Stations
police_df = spark.read.option("header", "true").csv(str(resolve_data_path(CFG, "police_stations")))
for old in police_df.columns:
    police_df = police_df.withColumnRenamed(old, old.replace(" ", "_"))

print("=== CORRELATION 1: Crime Rate vs Arrest Rate by District ===")
crime_by_district = crime_df.groupBy("District").agg(
    count("*").alias("crime_count")
)
arrest_by_district = crime_df.filter(col("Arrest") == "true").groupBy("District").agg(
    count("*").alias("arrest_count")
)
corr1 = crime_by_district.join(arrest_by_district, "District", "inner")
corr1 = corr1.withColumn("arrest_rate", col("arrest_count") / col("crime_count") * 100)
corr1.orderBy("District").show(15)

print("\n=== CORRELATION 2: Violence Incidents vs Arrest Rate by District ===")
violence_count = violence_df.groupBy("DISTRICT").agg(
    count("*").alias("violence_count")
).withColumnRenamed("DISTRICT", "District")
corr2 = crime_by_district.join(violence_count, "District", "inner")
corr2 = corr2.withColumn("arrest_rate", col("arrest_count") / col("crime_count") * 100)
corr2.select("District", "crime_count", "violence_count", "arrest_rate").show(15)

print("\n=== CORRELATION 3: Sex Offender Density by District ===")
sex_by_district = sex_df.groupBy("DISTRICT").agg(
    count("*").alias("sex_offender_count")
)
corr3 = crime_by_district.join(sex_by_district, crime_by_district.District == sex_by_district.DISTRICT, "inner")
corr3 = corr3.withColumn("crime_per_offender", col("crime_count") / col("sex_offender_count"))
corr3.select("District", "crime_count", "sex_offender_count", "crime_per_offender").show(15)

# Save to PostgreSQL
corr1.write.mode("overwrite").format("jdbc").options(
    url="jdbc:postgresql://postgres:5432/crime_analytics",
    driver="org.postgresql.Driver",
    dbtable="correlations",
    user="crime_user",
    password="crime_pass"
).save()

print("\n✅ Cross-correlations saved to PostgreSQL")
spark.stop()